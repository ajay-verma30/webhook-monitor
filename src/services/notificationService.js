'use strict';

/**
 * notificationService.js
 *
 * Sends threshold alerts and payment-lifecycle notifications over Email,
 * Slack, Discord, and Microsoft Teams.
 *
 * Anti-spam design:
 *  - Global per-gateway cooldown (default 30 min) stored in notification_configs.
 *  - Per-notification-type in-process deduplication (SENT_CACHE) prevents
 *    rapid repeated alerts within the same process restart cycle.
 *  - checkAndNotify() resolves gracefully when nothing is breached — it
 *    never throws, so callers can fire-and-forget.
 *
 * Scalability notes:
 *  - All channel sends are dispatched concurrently via Promise.allSettled —
 *    one slow/failing channel never blocks the others.
 *  - sendAllChannels() is the single dispatch point; adding a new channel
 *    means adding one entry here only.
 *  - Email template rendering is pure (no side effects) and easily unit-testable.
 */

const SibApiV3Sdk = require('sib-api-v3-sdk');
const axios       = require('axios');
const db          = require('../config/db');

// ─── Brevo (transactional email) client ───────────────────────────────────────

const brevoClient              = SibApiV3Sdk.ApiClient.instance;
brevoClient.authentications['api-key'].apiKey = process.env.BREVO_API_KEY;

const transactionalEmailsApi = new SibApiV3Sdk.TransactionalEmailsApi();

// ─── Constants ────────────────────────────────────────────────────────────────

/** Default thresholds used when a gateway has no custom notification config. */
const DEFAULT_THRESHOLDS = Object.freeze({
    amount_lost:   500,   // USD
    failed_events: 10,
});

/**
 * Minimum minutes between two notifications of the same type for the same gateway.
 * This in-process guard supplements the DB-level last_notified_at cooldown.
 */
const COOLDOWN_MINUTES = Object.freeze({
    threshold_breach: 30,
    retry_exhausted:  60,   // more severe — alert less often
    loss_recovered:    5,   // good news — OK to send sooner
});

const AXIOS_TIMEOUT_MS = 5_000;

/**
 * In-process deduplication cache.
 * Key: `${gatewayId}:${notificationType}`  →  Date (last sent)
 *
 * This is intentionally lightweight (no Redis/Memcached dependency).
 * For multi-instance deployments the DB-level cooldown is the authoritative guard;
 * this cache is just a fast first pass.
 */
const SENT_CACHE = new Map();

// ─── Template helpers ─────────────────────────────────────────────────────────

/** Shared HTML chrome for all email templates. */
const emailChrome = ({ headerBg, headerText, bodyContent }) => `
<div style="font-family:Inter,sans-serif;max-width:600px;margin:0 auto;">
  <div style="background:${headerBg};padding:24px;border-radius:12px 12px 0 0;">
    <h1 style="color:#fff;margin:0;font-size:20px;">${headerText}</h1>
  </div>
  <div style="background:#fff;padding:24px;border:1px solid #E9EDF5;border-top:none;border-radius:0 0 12px 12px;">
    ${bodyContent}
    <p style="color:#94A3B8;font-size:12px;margin-top:24px;">HookPulse — Webhook Reliability Platform</p>
  </div>
</div>`.trim();

/** Renders a two-column fact table row. */
const factRow = ({ label, value, bg = '#fff', valueColor = '#1A1D23' }) =>
    `<tr style="background:${bg};">
        <td style="padding:12px;color:#64748B;font-size:14px;">${label}</td>
        <td style="padding:12px;font-weight:bold;color:${valueColor};">${value}</td>
    </tr>`;

/** Renders a CTA button. */
const ctaButton = (label, url, bg) =>
    `<a href="${url}"
        style="display:inline-block;background:${bg};color:#fff;padding:12px 24px;
               border-radius:8px;text-decoration:none;font-weight:bold;margin-top:8px;">
        ${label} →
    </a>`;

/**
 * Returns the email { subject, html } for a given notification type.
 * Pure function — no side effects.
 *
 * @param {'threshold_breach'|'retry_exhausted'|'loss_recovered'} type
 * @param {object} data
 * @returns {{ subject: string, html: string }}
 */
const getEmailTemplate = (type, data) => {
    const dashboardUrl = `${process.env.FRONTEND_URL}/dashboard`;

    switch (type) {

        case 'threshold_breach': {
            const bannerHtml = `
                <div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:8px;padding:16px;margin:16px 0;">
                    <h2 style="color:#DC2626;margin:0 0 8px;font-size:16px;">Threshold Breached</h2>
                    <p style="color:#991B1B;margin:0;">${data.breach_message}</p>
                </div>`;
            const tableHtml = `
                <table style="width:100%;border-collapse:collapse;margin:16px 0;">
                    ${factRow({ label: 'Amount at Risk', value: `$${data.total_loss}`,  bg: '#F8F9FB', valueColor: '#DC2626' })}
                    ${factRow({ label: 'Failed Events',  value: data.failed_count })}
                    ${factRow({ label: 'Recovered',      value: `$${data.recovered}`,   bg: '#F8F9FB', valueColor: '#059669' })}
                </table>`;

            return {
                subject: `⚠️ HookPulse Alert — ${data.gateway_name}`,
                html: emailChrome({
                    headerBg:   '#0066FF',
                    headerText: '⚡ HookPulse Alert',
                    bodyContent: `
                        <p style="color:#64748B;margin-top:0;">Gateway: <strong>${data.gateway_name}</strong></p>
                        ${bannerHtml}${tableHtml}
                        ${ctaButton('View Dashboard', dashboardUrl, '#0066FF')}`,
                }),
            };
        }

        case 'retry_exhausted': {
            const eventUrl  = `${process.env.FRONTEND_URL}/log_details/${data.webhook_log_id}`;
            const bannerHtml = `
                <div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:8px;padding:16px;margin:16px 0;">
                    <p style="color:#991B1B;margin:0;">
                        All <strong>4 retry attempts</strong> failed for this payment.
                        Manual intervention required.
                    </p>
                </div>`;
            const tableHtml = `
                <table style="width:100%;border-collapse:collapse;margin:16px 0;">
                    ${factRow({ label: 'Event ID',      value: `<code>${data.event_id}</code>`, bg: '#F8F9FB' })}
                    ${factRow({ label: 'Amount Lost',   value: `$${data.amount}`, valueColor: '#DC2626' })}
                    ${factRow({ label: 'Decline Reason',value: data.decline_reason ?? 'Unknown', bg: '#F8F9FB' })}
                </table>`;

            return {
                subject: `❌ HookPulse — Retry Exhausted for ${data.event_id}`,
                html: emailChrome({
                    headerBg:   '#DC2626',
                    headerText: '❌ Retry Exhausted',
                    bodyContent: `
                        <p style="color:#64748B;margin-top:0;">Gateway: <strong>${data.gateway_name}</strong></p>
                        ${bannerHtml}${tableHtml}
                        ${ctaButton('View Event Details', eventUrl, '#DC2626')}`,
                }),
            };
        }

        case 'loss_recovered': {
            const bannerHtml = `
                <div style="background:#F0FDF4;border:1px solid #BBF7D0;border-radius:8px;padding:16px;margin:16px 0;">
                    <h2 style="color:#059669;margin:0 0 8px;font-size:16px;">$${data.amount} Successfully Recovered</h2>
                    <p style="color:#166534;margin:0;">Auto-retry attempt #${data.attempt_number} succeeded.</p>
                </div>`;

            return {
                subject: `✅ HookPulse — $${data.amount} Recovered on ${data.gateway_name}`,
                html: emailChrome({
                    headerBg:   '#059669',
                    headerText: '✅ Payment Recovered',
                    bodyContent: `
                        <p style="color:#64748B;margin-top:0;">Gateway: <strong>${data.gateway_name}</strong></p>
                        ${bannerHtml}
                        ${ctaButton('View Dashboard', dashboardUrl, '#059669')}`,
                }),
            };
        }

        default:
            throw new Error(`Unknown notification type: ${type}`);
    }
};

// ─── Slack payloads ───────────────────────────────────────────────────────────

const getSlackPayload = (type, data) => {
    const dashboardUrl = `${process.env.FRONTEND_URL}/dashboard`;

    const viewDashboard = {
        type: 'button',
        text: { type: 'plain_text', text: 'View Dashboard' },
        url:  dashboardUrl,
    };

    switch (type) {
        case 'threshold_breach':
            return {
                text: `⚠️ HookPulse Alert — ${data.gateway_name}`,
                blocks: [
                    { type: 'section', text: { type: 'mrkdwn', text: `*⚠️ Threshold Breached*\n${data.breach_message}` } },
                    { type: 'section', fields: [
                        { type: 'mrkdwn', text: `*Amount at Risk*\n$${data.total_loss}` },
                        { type: 'mrkdwn', text: `*Failed Events*\n${data.failed_count}` },
                        { type: 'mrkdwn', text: `*Recovered*\n$${data.recovered}`       },
                    ]},
                    { type: 'actions', elements: [viewDashboard] },
                ],
            };

        case 'retry_exhausted':
            return {
                text: `❌ Retry Exhausted — ${data.gateway_name}`,
                blocks: [
                    { type: 'section', text: { type: 'mrkdwn', text: '*All 4 retry attempts failed.* Manual intervention required.' } },
                    { type: 'section', fields: [
                        { type: 'mrkdwn', text: `*Event ID*\n\`${data.event_id}\`` },
                        { type: 'mrkdwn', text: `*Amount Lost*\n$${data.amount}` },
                        { type: 'mrkdwn', text: `*Decline*\n${data.decline_reason ?? 'Unknown'}` },
                    ]},
                    { type: 'actions', elements: [{
                        type: 'button',
                        text: { type: 'plain_text', text: 'View Event' },
                        url:  `${process.env.FRONTEND_URL}/log_details/${data.webhook_log_id}`,
                    }]},
                ],
            };

        case 'loss_recovered':
            return {
                text: `✅ $${data.amount} Recovered — ${data.gateway_name}`,
                blocks: [
                    { type: 'section', text: { type: 'mrkdwn', text: `Auto-retry attempt *#${data.attempt_number}* succeeded. $${data.amount} recovered! 🎉` } },
                    { type: 'actions', elements: [viewDashboard] },
                ],
            };

        default:
            throw new Error(`Unknown notification type: ${type}`);
    }
};

// ─── Discord payloads ─────────────────────────────────────────────────────────

const DISCORD_COLORS = Object.freeze({
    threshold_breach: 15_548_997,  // red
    retry_exhausted:  14_431_557,  // dark red
    loss_recovered:    5_763_719,  // green
});

const getDiscordPayload = (type, data) => {
    const ts = new Date().toISOString();

    switch (type) {
        case 'threshold_breach':
            return {
                username: 'HookPulse',
                embeds: [{
                    title:       `⚠️ Threshold Breached — ${data.gateway_name}`,
                    description: data.breach_message,
                    color:       DISCORD_COLORS.threshold_breach,
                    fields: [
                        { name: 'Amount at Risk', value: `$${data.total_loss}`,  inline: true },
                        { name: 'Failed Events',  value: `${data.failed_count}`, inline: true },
                        { name: 'Recovered',      value: `$${data.recovered}`,   inline: true },
                    ],
                    timestamp: ts,
                }],
            };

        case 'retry_exhausted':
            return {
                username: 'HookPulse',
                embeds: [{
                    title:       `❌ Retry Exhausted — ${data.gateway_name}`,
                    description: 'All 4 retry attempts failed. Manual intervention required.',
                    color:       DISCORD_COLORS.retry_exhausted,
                    fields: [
                        { name: 'Event ID',    value: data.event_id,                    inline: false },
                        { name: 'Amount Lost', value: `$${data.amount}`,                inline: true  },
                        { name: 'Decline',     value: data.decline_reason ?? 'Unknown', inline: true  },
                    ],
                    timestamp: ts,
                }],
            };

        case 'loss_recovered':
            return {
                username: 'HookPulse',
                embeds: [{
                    title:       `✅ Payment Recovered — ${data.gateway_name}`,
                    description: `Auto-retry #${data.attempt_number} succeeded. $${data.amount} recovered!`,
                    color:       DISCORD_COLORS.loss_recovered,
                    timestamp: ts,
                }],
            };

        default:
            throw new Error(`Unknown notification type: ${type}`);
    }
};

// ─── Teams payloads ───────────────────────────────────────────────────────────

const getTeamsPayload = (type, data) => {
    const dashboardUrl = `${process.env.FRONTEND_URL}/dashboard`;
    const openUrl = (title, url) => ({ type: 'Action.OpenUrl', title, url });

    const card = (body, actions) => ({
        type: 'message',
        attachments: [{
            contentType: 'application/vnd.microsoft.card.adaptive',
            content: { type: 'AdaptiveCard', version: '1.4', body, actions },
        }],
    });

    switch (type) {
        case 'threshold_breach':
            return card(
                [
                    { type: 'TextBlock', text: `⚠️ HookPulse Alert — ${data.gateway_name}`, weight: 'Bolder', size: 'Medium', color: 'Attention' },
                    { type: 'TextBlock', text: data.breach_message, wrap: true },
                    { type: 'FactSet',   facts: [
                        { title: 'Amount at Risk', value: `$${data.total_loss}`   },
                        { title: 'Failed Events',  value: `${data.failed_count}`  },
                        { title: 'Recovered',      value: `$${data.recovered}`    },
                    ]},
                ],
                [openUrl('View Dashboard', dashboardUrl)],
            );

        case 'retry_exhausted':
            return card(
                [
                    { type: 'TextBlock', text: `❌ Retry Exhausted — ${data.gateway_name}`, weight: 'Bolder', size: 'Medium', color: 'Attention' },
                    { type: 'TextBlock', text: 'All 4 retry attempts failed. Manual intervention required.', wrap: true },
                    { type: 'FactSet',   facts: [
                        { title: 'Event ID',    value: data.event_id                    },
                        { title: 'Amount Lost', value: `$${data.amount}`                },
                        { title: 'Decline',     value: data.decline_reason ?? 'Unknown' },
                    ]},
                ],
                [openUrl('View Event', `${process.env.FRONTEND_URL}/log_details/${data.webhook_log_id}`)],
            );

        case 'loss_recovered':
            return card(
                [
                    { type: 'TextBlock', text: `✅ Payment Recovered — ${data.gateway_name}`, weight: 'Bolder', size: 'Medium', color: 'Good' },
                    { type: 'TextBlock', text: `Auto-retry #${data.attempt_number} succeeded. $${data.amount} recovered!`, wrap: true },
                ],
                [openUrl('View Dashboard', dashboardUrl)],
            );

        default:
            throw new Error(`Unknown notification type: ${type}`);
    }
};

// ─── Anti-spam gate ───────────────────────────────────────────────────────────

/**
 * Returns true when a notification is allowed to fire, updating the in-process
 * cache if it is.  Does NOT touch the database — DB cooldown is enforced separately
 * in checkAndNotify() so the two guards are independent layers.
 *
 * @param {string} gatewayId
 * @param {string} type
 * @returns {boolean}
 */
const isAllowedByCache = (gatewayId, type) => {
    const key           = `${gatewayId}:${type}`;
    const lastSent      = SENT_CACHE.get(key);
    const cooldownMs    = (COOLDOWN_MINUTES[type] ?? 30) * 60 * 1_000;

    if (lastSent && Date.now() - lastSent < cooldownMs) return false;

    SENT_CACHE.set(key, Date.now());
    return true;
};

/**
 * Returns true when the DB-level cooldown has expired (or was never set).
 * Updates last_notified_at on the notification_config row if the guard passes.
 *
 * @param {object} config - row from notification_configs
 * @param {string} type   - notification type key
 * @returns {Promise<boolean>}
 */
const isAllowedByDb = async (config, type) => {
    if (!config.last_notified_at) return true;

    const cooldownMs  = (COOLDOWN_MINUTES[type] ?? 30) * 60 * 1_000;
    const elapsed     = Date.now() - new Date(config.last_notified_at).getTime();

    return elapsed >= cooldownMs;
};

/**
 * Persists last_notified_at to prevent re-alerts until the cooldown expires.
 */
const markNotified = async (configId) => {
    await db.query(
        'UPDATE notification_configs SET last_notified_at = NOW() WHERE id = $1',
        [configId],
    );
};

// ─── Channel senders ──────────────────────────────────────────────────────────

/**
 * Sends a transactional email via Brevo.
 *
 * @param {{ to: string, subject: string, html: string }} params
 */
const sendEmail = async ({ to, subject, html }) => {
    try {
        const msg          = new SibApiV3Sdk.SendSmtpEmail();
        msg.sender         = {
            name:  process.env.BREVO_SENDER_NAME  ?? 'HookPulse',
            email: process.env.BREVO_SENDER_EMAIL ?? 'noreply@hookpulse.com',
        };
        msg.to             = [{ email: to }];
        msg.subject        = subject;
        msg.htmlContent    = html;

        await transactionalEmailsApi.sendTransacEmail(msg);
        console.log(`📧 Email sent → ${to}`);
    } catch (err) {
        console.error('❌ Email failed:', err.response?.body ?? err.message);
    }
};

/**
 * Posts a Slack Block Kit message to an incoming webhook URL.
 */
const sendSlack = async (webhookUrl, type, data) => {
    try {
        await axios.post(webhookUrl, getSlackPayload(type, data), { timeout: AXIOS_TIMEOUT_MS });
        console.log(`💬 Slack sent — ${type}`);
    } catch (err) {
        console.error('❌ Slack failed:', err.response?.data ?? err.message);
    }
};

/**
 * Posts a Discord embed to an incoming webhook URL.
 */
const sendDiscord = async (webhookUrl, type, data) => {
    try {
        await axios.post(webhookUrl, getDiscordPayload(type, data), { timeout: AXIOS_TIMEOUT_MS });
        console.log(`🎮 Discord sent — ${type}`);
    } catch (err) {
        console.error('❌ Discord failed:', err.message);
    }
};

/**
 * Posts an Adaptive Card to a Microsoft Teams incoming webhook URL.
 */
const sendTeams = async (webhookUrl, type, data) => {
    try {
        await axios.post(webhookUrl, getTeamsPayload(type, data), { timeout: AXIOS_TIMEOUT_MS });
        console.log(`🏢 Teams sent — ${type}`);
    } catch (err) {
        console.error('❌ Teams failed:', err.message);
    }
};

/**
 * Dispatches a notification to every configured channel concurrently.
 * Promise.allSettled ensures one failing channel never blocks the others.
 *
 * @param {object} config - notification_configs row (joined with gateway + user)
 * @param {string} type   - notification type key
 * @param {object} data   - template data object
 */
const sendAllChannels = async (config, type, data) => {
    const template = getEmailTemplate(type, data);

    await Promise.allSettled([
        config.email_enabled
            ? sendEmail({ to: config.user_email, ...template })
            : null,

        config.slack_enabled && config.slack_webhook_url
            ? sendSlack(config.slack_webhook_url, type, data)
            : null,

        config.discord_enabled && config.discord_webhook_url
            ? sendDiscord(config.discord_webhook_url, type, data)
            : null,

        config.teams_enabled && config.teams_webhook_url
            ? sendTeams(config.teams_webhook_url, type, data)
            : null,
    ].filter(Boolean));
};

// ─── DB helpers ───────────────────────────────────────────────────────────────

/**
 * Fetches the notification config for a gateway, including the user email and
 * gateway name.  Returns null if no config exists.
 *
 * @param {string} gatewayId
 * @returns {Promise<object|null>}
 */
const getNotifConfig = async (gatewayId) => {
    const { rows } = await db.query(
        `SELECT
            nc.*,
            u.email AS user_email,
            g.name  AS gateway_name
         FROM notification_configs nc
         JOIN gateways g ON nc.gateway_id = g.id
         JOIN users    u ON nc.user_id    = u.id
         WHERE nc.gateway_id = $1
         LIMIT 1`,
        [gatewayId],
    );
    return rows[0] ?? null;
};

// ─── Public triggers ──────────────────────────────────────────────────────────

/**
 * Evaluates loss/failure thresholds for a gateway and fires a notification if
 * either is breached — subject to both the in-process cache and the DB cooldown.
 *
 * Never throws — callers may fire-and-forget.
 *
 * @param {{ gateway_id: string, range?: string }} options
 */
const checkAndNotify = async ({ gateway_id, range = '24 hours' }) => {
    try {
        const config = await getNotifConfig(gateway_id);
        if (!config || !config.notify_threshold_breach) return;

        // Fast path — in-process cache check (no DB round-trip)
        if (!isAllowedByCache(gateway_id, 'threshold_breach')) return;

        // Slower path — DB cooldown check
        if (!(await isAllowedByDb(config, 'threshold_breach'))) return;

        // ── Gather current stats ───────────────────────────────────────
        const [statsRes, recoveryRes] = await Promise.all([
            db.query(
                `SELECT
                    COUNT(*) FILTER (WHERE status IN ('FAILED','CRITICAL'))    AS failed_count,
                    COALESCE(SUM(
                        CASE WHEN event_type = 'charge.failed'
                        THEN (payload #>> '{data,object,amount}')::numeric / 100
                        ELSE 0 END
                    ), 0)                                                       AS total_loss
                 FROM webhook_logs
                 WHERE gateway_id  = $1
                   AND received_at >= NOW() - INTERVAL '${range}'
                   AND status     != 'IGNORED'`,
                [gateway_id],
            ),
            db.query(
                `SELECT COALESCE(SUM(amount), 0) AS total_recovered
                 FROM recovery_logs
                 WHERE gateway_id   = $1
                   AND recovered_at >= NOW() - INTERVAL '${range}'`,
                [gateway_id],
            ),
        ]);

        const failedCount = parseInt(statsRes.rows[0].failed_count ?? 0, 10);
        const totalLoss   = parseFloat(statsRes.rows[0].total_loss  ?? 0);
        const recovered   = parseFloat(recoveryRes.rows[0].total_recovered ?? 0);

        const amountThreshold  = config.amount_threshold  ?? DEFAULT_THRESHOLDS.amount_lost;
        const failureThreshold = config.failure_threshold ?? DEFAULT_THRESHOLDS.failed_events;

        const amountBreached = totalLoss   >= amountThreshold;
        const countBreached  = failedCount >= failureThreshold;

        if (!amountBreached && !countBreached) return;

        // ── Build breach message ───────────────────────────────────────
        const breachParts = [];
        if (amountBreached) breachParts.push(`Amount lost ($${totalLoss.toFixed(2)}) exceeded $${amountThreshold} threshold`);
        if (countBreached)  breachParts.push(`Failed events (${failedCount}) exceeded ${failureThreshold} threshold`);

        const data = {
            gateway_name:   config.gateway_name,
            breach_message: breachParts.join(' & '),
            total_loss:     totalLoss.toFixed(2),
            failed_count:   failedCount,
            recovered:      recovered.toFixed(2),
        };

        await sendAllChannels(config, 'threshold_breach', data);
        await markNotified(config.id);

    } catch (err) {
        console.error('❌ checkAndNotify failed:', err.message);
    }
};

/**
 * Fires a "retry exhausted" notification when all auto-retry attempts have failed.
 *
 * @param {{ gateway_id, webhookLogId, eventId, amount, declineReason }} params
 */
const notifyRetryExhausted = async ({ gateway_id, webhookLogId, eventId, amount, declineReason }) => {
    try {
        const config = await getNotifConfig(gateway_id);
        if (!config || !config.notify_retry_exhausted) return;

        if (!isAllowedByCache(gateway_id, 'retry_exhausted')) return;
        if (!(await isAllowedByDb(config, 'retry_exhausted'))) return;

        const data = {
            gateway_name:   config.gateway_name,
            event_id:       eventId,
            webhook_log_id: webhookLogId,
            amount:         amount?.toFixed(2) ?? '0.00',
            decline_reason: declineReason,
        };

        await sendAllChannels(config, 'retry_exhausted', data);
        await markNotified(config.id);

    } catch (err) {
        console.error('❌ notifyRetryExhausted failed:', err.message);
    }
};

/**
 * Fires a "loss recovered" notification when an auto-retry succeeds.
 *
 * @param {{ gateway_id, amount, attemptNumber }} params
 */
const notifyLossRecovered = async ({ gateway_id, amount, attemptNumber }) => {
    try {
        const config = await getNotifConfig(gateway_id);
        if (!config || !config.notify_loss_recovered) return;

        if (!isAllowedByCache(gateway_id, 'loss_recovered')) return;
        if (!(await isAllowedByDb(config, 'loss_recovered'))) return;

        const data = {
            gateway_name:   config.gateway_name,
            amount:         amount?.toFixed(2) ?? '0.00',
            attempt_number: attemptNumber,
        };

        await sendAllChannels(config, 'loss_recovered', data);
        await markNotified(config.id);

    } catch (err) {
        console.error('❌ notifyLossRecovered failed:', err.message);
    }
};

// ─── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
    sendEmail,
    checkAndNotify,
    notifyRetryExhausted,
    notifyLossRecovered,
};