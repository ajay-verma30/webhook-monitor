const Brevo = require('@getbrevo/brevo');
const axios = require('axios');
const db = require('../config/db');

// ─────────────────────────────────────────────
// BREVO CLIENT SETUP
// ─────────────────────────────────────────────
const brevoClient = Brevo.ApiClient.instance;
const apiKey = brevoClient.authentications['api-key'];
apiKey.apiKey = process.env.BREVO_API_KEY;

const transactionalEmailsApi = new Brevo.TransactionalEmailsApi();

// ─────────────────────────────────────────────
// DEFAULT THRESHOLDS
// These apply if user hasn't configured custom ones
// ─────────────────────────────────────────────
const DEFAULT_THRESHOLDS = {
    amount_lost:     500,   // $500
    failed_events:   10,    // 10 failed events
};


// ─────────────────────────────────────────────
// EMAIL TEMPLATES
// ─────────────────────────────────────────────
const getEmailTemplate = (type, data) => {
    const templates = {

        // ── Threshold Breach ──────────────────────────────────────────
        threshold_breach: {
            subject: `⚠️ HookPulse Alert — ${data.gateway_name}`,
            html: `
                <div style="font-family: Inter, sans-serif; max-width: 600px; margin: 0 auto;">
                    <div style="background: #0066FF; padding: 24px; border-radius: 12px 12px 0 0;">
                        <h1 style="color: white; margin: 0; font-size: 20px;">⚡ HookPulse Alert</h1>
                    </div>
                    <div style="background: #fff; padding: 24px; border: 1px solid #E9EDF5; border-top: none; border-radius: 0 0 12px 12px;">
                        <p style="color: #64748B; margin-top: 0;">Gateway: <strong>${data.gateway_name}</strong></p>
                        <div style="background: #FEF2F2; border: 1px solid #FECACA; border-radius: 8px; padding: 16px; margin: 16px 0;">
                            <h2 style="color: #DC2626; margin: 0 0 8px 0; font-size: 16px;">Threshold Breached</h2>
                            <p style="color: #991B1B; margin: 0;">${data.breach_message}</p>
                        </div>
                        <table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
                            <tr style="background: #F8F9FB;">
                                <td style="padding: 12px; border-radius: 8px 0 0 8px; color: #64748B; font-size: 14px;">Amount at Risk</td>
                                <td style="padding: 12px; font-weight: bold; color: #DC2626;">$${data.total_loss}</td>
                            </tr>
                            <tr>
                                <td style="padding: 12px; color: #64748B; font-size: 14px;">Failed Events</td>
                                <td style="padding: 12px; font-weight: bold; color: #1A1D23;">${data.failed_count}</td>
                            </tr>
                            <tr style="background: #F8F9FB;">
                                <td style="padding: 12px; border-radius: 8px 0 0 8px; color: #64748B; font-size: 14px;">Recovered</td>
                                <td style="padding: 12px; font-weight: bold; color: #059669;">$${data.recovered}</td>
                            </tr>
                        </table>
                        <a href="${process.env.FRONTEND_URL}/dashboard" 
                           style="display: inline-block; background: #0066FF; color: white; padding: 12px 24px; border-radius: 8px; text-decoration: none; font-weight: bold; margin-top: 8px;">
                            View Dashboard →
                        </a>
                        <p style="color: #94A3B8; font-size: 12px; margin-top: 24px;">HookPulse — Webhook Reliability Platform</p>
                    </div>
                </div>
            `,
        },

        // ── Retry Exhausted ───────────────────────────────────────────
        retry_exhausted: {
            subject: `❌ HookPulse — Retry Exhausted for Payment ${data.payment_intent_id || data.event_id}`,
            html: `
                <div style="font-family: Inter, sans-serif; max-width: 600px; margin: 0 auto;">
                    <div style="background: #DC2626; padding: 24px; border-radius: 12px 12px 0 0;">
                        <h1 style="color: white; margin: 0; font-size: 20px;">❌ Retry Exhausted</h1>
                    </div>
                    <div style="background: #fff; padding: 24px; border: 1px solid #E9EDF5; border-top: none; border-radius: 0 0 12px 12px;">
                        <p style="color: #64748B; margin-top: 0;">Gateway: <strong>${data.gateway_name}</strong></p>
                        <div style="background: #FEF2F2; border: 1px solid #FECACA; border-radius: 8px; padding: 16px; margin: 16px 0;">
                            <p style="color: #991B1B; margin: 0;">
                                All <strong>4 retry attempts</strong> failed for this payment. 
                                Manual intervention required.
                            </p>
                        </div>
                        <table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
                            <tr style="background: #F8F9FB;">
                                <td style="padding: 12px; color: #64748B; font-size: 14px;">Event ID</td>
                                <td style="padding: 12px; font-weight: bold; font-family: monospace; font-size: 13px;">${data.event_id}</td>
                            </tr>
                            <tr>
                                <td style="padding: 12px; color: #64748B; font-size: 14px;">Amount Lost</td>
                                <td style="padding: 12px; font-weight: bold; color: #DC2626;">$${data.amount}</td>
                            </tr>
                            <tr style="background: #F8F9FB;">
                                <td style="padding: 12px; color: #64748B; font-size: 14px;">Decline Reason</td>
                                <td style="padding: 12px; color: #1A1D23;">${data.decline_reason || 'Unknown'}</td>
                            </tr>
                        </table>
                        <a href="${process.env.FRONTEND_URL}/log_details/${data.webhook_log_id}" 
                           style="display: inline-block; background: #DC2626; color: white; padding: 12px 24px; border-radius: 8px; text-decoration: none; font-weight: bold; margin-top: 8px;">
                            View Event Details →
                        </a>
                        <p style="color: #94A3B8; font-size: 12px; margin-top: 24px;">HookPulse — Webhook Reliability Platform</p>
                    </div>
                </div>
            `,
        },

        // ── Loss Recovered ────────────────────────────────────────────
        loss_recovered: {
            subject: `✅ HookPulse — $${data.amount} Recovered on ${data.gateway_name}`,
            html: `
                <div style="font-family: Inter, sans-serif; max-width: 600px; margin: 0 auto;">
                    <div style="background: #059669; padding: 24px; border-radius: 12px 12px 0 0;">
                        <h1 style="color: white; margin: 0; font-size: 20px;">✅ Payment Recovered</h1>
                    </div>
                    <div style="background: #fff; padding: 24px; border: 1px solid #E9EDF5; border-top: none; border-radius: 0 0 12px 12px;">
                        <p style="color: #64748B; margin-top: 0;">Gateway: <strong>${data.gateway_name}</strong></p>
                        <div style="background: #F0FDF4; border: 1px solid #BBF7D0; border-radius: 8px; padding: 16px; margin: 16px 0;">
                            <h2 style="color: #059669; margin: 0 0 8px 0; font-size: 16px;">$${data.amount} Successfully Recovered</h2>
                            <p style="color: #166534; margin: 0;">
                                Auto-retry attempt #${data.attempt_number} succeeded.
                            </p>
                        </div>
                        <a href="${process.env.FRONTEND_URL}/dashboard" 
                           style="display: inline-block; background: #059669; color: white; padding: 12px 24px; border-radius: 8px; text-decoration: none; font-weight: bold; margin-top: 8px;">
                            View Dashboard →
                        </a>
                        <p style="color: #94A3B8; font-size: 12px; margin-top: 24px;">HookPulse — Webhook Reliability Platform</p>
                    </div>
                </div>
            `,
        },
    };

    return templates[type];
};


// ─────────────────────────────────────────────
// CORE: Send Email via Brevo
// ─────────────────────────────────────────────
const sendEmail = async ({ to, subject, html }) => {
    try {
        const sendSmtpEmail = new Brevo.SendSmtpEmail();

        sendSmtpEmail.sender = {
            name:  process.env.BREVO_SENDER_NAME  || 'HookPulse',
            email: process.env.BREVO_SENDER_EMAIL || 'noreply@hookpulse.com',
        };
        sendSmtpEmail.to      = [{ email: to }];
        sendSmtpEmail.subject = subject;
        sendSmtpEmail.htmlContent = html;

        await transactionalEmailsApi.sendTransacEmail(sendSmtpEmail);
        console.log(`📧 Email sent to ${to} — ${subject}`);
        return true;
    } catch (err) {
        console.error('❌ Email send failed:', err.message);
        return false;
    }
};


// ─────────────────────────────────────────────
// HELPER: Get notification config for gateway
// Fetches from notification_configs table
// ─────────────────────────────────────────────
const getNotifConfig = async (gateway_id) => {
    const res = await db.query(
        `SELECT 
            nc.*,
            u.email AS user_email,
            g.name  AS gateway_name,
            g.amount_threshold  AS gw_amount_threshold,
            g.failure_threshold AS gw_failure_threshold
         FROM notification_configs nc
         JOIN gateways g ON nc.gateway_id = g.id
         JOIN users u    ON nc.user_id = u.id
         WHERE nc.gateway_id = $1
         LIMIT 1`,
        [gateway_id]
    );
    return res.rows[0] || null;
};


// ─────────────────────────────────────────────
// TRIGGER: Check thresholds and notify
// Called after every webhook event
// ─────────────────────────────────────────────
const checkAndNotify = async ({ gateway_id, range = '24 hours' }) => {
    try {
        const config = await getNotifConfig(gateway_id);
        if (!config) return;

        if (!config.notify_threshold_breach) return;

        // Get current stats
        const statsRes = await db.query(
            `SELECT
                COUNT(*) FILTER (WHERE status IN ('FAILED', 'CRITICAL'))    AS failed_count,
                COALESCE(SUM(
                    CASE WHEN event_type = 'charge.failed' 
                    THEN (payload #>> '{data,object,amount}')::numeric / 100 
                    ELSE 0 END
                ), 0) AS total_loss
             FROM webhook_logs
             WHERE gateway_id = $1
             AND received_at >= NOW() - INTERVAL '${range}'
             AND status != 'IGNORED'`,
            [gateway_id]
        );

        const recoveryRes = await db.query(
            `SELECT COALESCE(SUM(amount), 0) AS total_recovered
             FROM recovery_logs
             WHERE gateway_id = $1
             AND recovered_at >= NOW() - INTERVAL '${range}'`,
            [gateway_id]
        );

        const failedCount  = parseInt(statsRes.rows[0].failed_count || 0);
        const totalLoss    = parseFloat(statsRes.rows[0].total_loss || 0);
        const recovered    = parseFloat(recoveryRes.rows[0].total_recovered || 0);

        // Use user-configured thresholds or defaults
        const amountThreshold  = config.amount_threshold  || DEFAULT_THRESHOLDS.amount_lost;
        const failureThreshold = config.failure_threshold || DEFAULT_THRESHOLDS.failed_events;

        const amountBreached = totalLoss >= amountThreshold;
        const countBreached  = failedCount >= failureThreshold;

        if (amountBreached || countBreached) {
            const breachMessages = [];
            if (amountBreached) breachMessages.push(`Amount lost ($${totalLoss.toFixed(2)}) exceeded $${amountThreshold} threshold`);
            if (countBreached)  breachMessages.push(`Failed events (${failedCount}) exceeded ${failureThreshold} threshold`);

            const data = {
                gateway_name:   config.gateway_name,
                breach_message: breachMessages.join(' & '),
                total_loss:     totalLoss.toFixed(2),
                failed_count:   failedCount,
                recovered:      recovered.toFixed(2),
            };

            await sendAllChannels(config, 'threshold_breach', data);
        }

    } catch (err) {
        console.error('❌ Notification check failed:', err.message);
    }
};


// ─────────────────────────────────────────────
// TRIGGER: Retry Exhausted Notification
// ─────────────────────────────────────────────
const notifyRetryExhausted = async ({ gateway_id, webhookLogId, eventId, amount, declineReason }) => {
    try {
        const config = await getNotifConfig(gateway_id);
        if (!config || !config.notify_retry_exhausted) return;

        const data = {
            gateway_name:   config.gateway_name,
            event_id:       eventId,
            webhook_log_id: webhookLogId,
            amount:         amount?.toFixed(2) || '0.00',
            decline_reason: declineReason,
        };

        await sendAllChannels(config, 'retry_exhausted', data);

    } catch (err) {
        console.error('❌ Retry exhausted notification failed:', err.message);
    }
};


// ─────────────────────────────────────────────
// TRIGGER: Loss Recovered Notification
// ─────────────────────────────────────────────
const notifyLossRecovered = async ({ gateway_id, amount, attemptNumber }) => {
    try {
        const config = await getNotifConfig(gateway_id);
        if (!config || !config.notify_loss_recovered) return;

        const data = {
            gateway_name:   config.gateway_name,
            amount:         amount?.toFixed(2) || '0.00',
            attempt_number: attemptNumber,
        };

        await sendAllChannels(config, 'loss_recovered', data);

    } catch (err) {
        console.error('❌ Loss recovered notification failed:', err.message);
    }
};



// ─────────────────────────────────────────────
// SLACK MESSAGE BLOCKS
// ─────────────────────────────────────────────
const getSlackPayload = (type, data) => {
    const colors = {
        threshold_breach: '#EF4444',
        retry_exhausted:  '#DC2626',
        loss_recovered:   '#10B981',
    };

    const messages = {
        threshold_breach: {
            text: `⚠️ *HookPulse Alert — ${data.gateway_name}*`,
            attachments: [{
                color: colors.threshold_breach,
                blocks: [
                    { type: 'section', text: { type: 'mrkdwn', text: `*Threshold Breached*\n${data.breach_message}` }},
                    { type: 'section', fields: [
                        { type: 'mrkdwn', text: `*Amount at Risk*\n$${data.total_loss}` },
                        { type: 'mrkdwn', text: `*Failed Events*\n${data.failed_count}` },
                        { type: 'mrkdwn', text: `*Recovered*\n$${data.recovered}` },
                    ]},
                    { type: 'actions', elements: [{
                        type: 'button', text: { type: 'plain_text', text: 'View Dashboard' },
                        url: `${process.env.FRONTEND_URL}/dashboard`, style: 'danger'
                    }]}
                ]
            }]
        },
        retry_exhausted: {
            text: `❌ *Retry Exhausted — ${data.gateway_name}*`,
            attachments: [{
                color: colors.retry_exhausted,
                blocks: [
                    { type: 'section', text: { type: 'mrkdwn', text: `All *4 retry attempts* failed. Manual intervention required.` }},
                    { type: 'section', fields: [
                        { type: 'mrkdwn', text: `*Event ID*\n${data.event_id}` },
                        { type: 'mrkdwn', text: `*Amount Lost*\n$${data.amount}` },
                        { type: 'mrkdwn', text: `*Decline Reason*\n${data.decline_reason || 'Unknown'}` },
                    ]},
                    { type: 'actions', elements: [{
                        type: 'button', text: { type: 'plain_text', text: 'View Event' },
                        url: `${process.env.FRONTEND_URL}/log_details/${data.webhook_log_id}`
                    }]}
                ]
            }]
        },
        loss_recovered: {
            text: `✅ *$${data.amount} Recovered — ${data.gateway_name}*`,
            attachments: [{
                color: colors.loss_recovered,
                blocks: [
                    { type: 'section', text: { type: 'mrkdwn', text: `Auto-retry attempt *#${data.attempt_number}* succeeded. Payment recovered!` }},
                    { type: 'actions', elements: [{
                        type: 'button', text: { type: 'plain_text', text: 'View Dashboard' },
                        url: `${process.env.FRONTEND_URL}/dashboard`, style: 'primary'
                    }]}
                ]
            }]
        },
    };

    return messages[type];
};


// ─────────────────────────────────────────────
// DISCORD MESSAGE EMBEDS
// ─────────────────────────────────────────────
const getDiscordPayload = (type, data) => {
    const colors = {
        threshold_breach: 15548997,  // red
        retry_exhausted:  14431557,  // dark red
        loss_recovered:   5763719,   // green
    };

    const embeds = {
        threshold_breach: {
            username: 'HookPulse',
            embeds: [{
                title: `⚠️ Threshold Breached — ${data.gateway_name}`,
                description: data.breach_message,
                color: colors.threshold_breach,
                fields: [
                    { name: 'Amount at Risk', value: `$${data.total_loss}`, inline: true },
                    { name: 'Failed Events',  value: `${data.failed_count}`, inline: true },
                    { name: 'Recovered',      value: `$${data.recovered}`,  inline: true },
                ],
                timestamp: new Date().toISOString(),
            }]
        },
        retry_exhausted: {
            username: 'HookPulse',
            embeds: [{
                title: `❌ Retry Exhausted — ${data.gateway_name}`,
                description: 'All 4 retry attempts failed. Manual intervention required.',
                color: colors.retry_exhausted,
                fields: [
                    { name: 'Event ID',      value: data.event_id,                   inline: false },
                    { name: 'Amount Lost',   value: `$${data.amount}`,               inline: true  },
                    { name: 'Decline',       value: data.decline_reason || 'Unknown', inline: true  },
                ],
                timestamp: new Date().toISOString(),
            }]
        },
        loss_recovered: {
            username: 'HookPulse',
            embeds: [{
                title: `✅ Payment Recovered — ${data.gateway_name}`,
                description: `Auto-retry #${data.attempt_number} succeeded. $${data.amount} recovered!`,
                color: colors.loss_recovered,
                timestamp: new Date().toISOString(),
            }]
        },
    };

    return embeds[type];
};


// ─────────────────────────────────────────────
// TEAMS MESSAGE CARDS
// ─────────────────────────────────────────────
const getTeamsPayload = (type, data) => {
    const themes = {
        threshold_breach: 'attention',
        retry_exhausted:  'attention',
        loss_recovered:   'good',
    };

    const cards = {
        threshold_breach: {
            type: 'message',
            attachments: [{
                contentType: 'application/vnd.microsoft.card.adaptive',
                content: {
                    type: 'AdaptiveCard', version: '1.4',
                    body: [
                        { type: 'TextBlock', text: `⚠️ HookPulse Alert — ${data.gateway_name}`, weight: 'Bolder', size: 'Medium', color: 'Attention' },
                        { type: 'TextBlock', text: data.breach_message, wrap: true },
                        { type: 'FactSet', facts: [
                            { title: 'Amount at Risk', value: `$${data.total_loss}` },
                            { title: 'Failed Events',  value: `${data.failed_count}` },
                            { title: 'Recovered',      value: `$${data.recovered}`  },
                        ]},
                    ],
                    actions: [{ type: 'Action.OpenUrl', title: 'View Dashboard', url: `${process.env.FRONTEND_URL}/dashboard` }]
                }
            }]
        },
        retry_exhausted: {
            type: 'message',
            attachments: [{
                contentType: 'application/vnd.microsoft.card.adaptive',
                content: {
                    type: 'AdaptiveCard', version: '1.4',
                    body: [
                        { type: 'TextBlock', text: `❌ Retry Exhausted — ${data.gateway_name}`, weight: 'Bolder', size: 'Medium', color: 'Attention' },
                        { type: 'TextBlock', text: 'All 4 retry attempts failed. Manual intervention required.', wrap: true },
                        { type: 'FactSet', facts: [
                            { title: 'Event ID',    value: data.event_id },
                            { title: 'Amount Lost', value: `$${data.amount}` },
                            { title: 'Decline',     value: data.decline_reason || 'Unknown' },
                        ]},
                    ],
                    actions: [{ type: 'Action.OpenUrl', title: 'View Event', url: `${process.env.FRONTEND_URL}/log_details/${data.webhook_log_id}` }]
                }
            }]
        },
        loss_recovered: {
            type: 'message',
            attachments: [{
                contentType: 'application/vnd.microsoft.card.adaptive',
                content: {
                    type: 'AdaptiveCard', version: '1.4',
                    body: [
                        { type: 'TextBlock', text: `✅ Payment Recovered — ${data.gateway_name}`, weight: 'Bolder', size: 'Medium', color: 'Good' },
                        { type: 'TextBlock', text: `Auto-retry #${data.attempt_number} succeeded. $${data.amount} recovered!`, wrap: true },
                    ],
                    actions: [{ type: 'Action.OpenUrl', title: 'View Dashboard', url: `${process.env.FRONTEND_URL}/dashboard` }]
                }
            }]
        },
    };

    return cards[type];
};


// ─────────────────────────────────────────────
// CORE: Send to Slack
// ─────────────────────────────────────────────
const sendSlack = async (webhookUrl, type, data) => {
    if (!webhookUrl) return;
    try {
        await axios.post(webhookUrl, getSlackPayload(type, data));
        console.log(`💬 Slack notification sent — ${type}`);
    } catch (err) {
        console.error('❌ Slack notification failed:', err.message);
    }
};


// ─────────────────────────────────────────────
// CORE: Send to Discord
// ─────────────────────────────────────────────
const sendDiscord = async (webhookUrl, type, data) => {
    if (!webhookUrl) return;
    try {
        await axios.post(webhookUrl, getDiscordPayload(type, data));
        console.log(`🎮 Discord notification sent — ${type}`);
    } catch (err) {
        console.error('❌ Discord notification failed:', err.message);
    }
};


// ─────────────────────────────────────────────
// CORE: Send to Teams
// ─────────────────────────────────────────────
const sendTeams = async (webhookUrl, type, data) => {
    if (!webhookUrl) return;
    try {
        await axios.post(webhookUrl, getTeamsPayload(type, data));
        console.log(`🏢 Teams notification sent — ${type}`);
    } catch (err) {
        console.error('❌ Teams notification failed:', err.message);
    }
};


// ─────────────────────────────────────────────
// CORE: Send All Configured Channels
// ─────────────────────────────────────────────
const sendAllChannels = async (config, type, data) => {
    await Promise.allSettled([
        // Email — always send if email_enabled
        config.email_enabled
            ? sendEmail({ to: config.user_email, ...getEmailTemplate(type, data) })
            : Promise.resolve(),

        // Slack
        config.slack_enabled && config.slack_webhook_url
            ? sendSlack(config.slack_webhook_url, type, data)
            : Promise.resolve(),

        // Discord
        config.discord_enabled && config.discord_webhook_url
            ? sendDiscord(config.discord_webhook_url, type, data)
            : Promise.resolve(),

        // Teams
        config.teams_enabled && config.teams_webhook_url
            ? sendTeams(config.teams_webhook_url, type, data)
            : Promise.resolve(),
    ]);
};

module.exports = {
    sendEmail,
    checkAndNotify,
    notifyRetryExhausted,
    notifyLossRecovered,
};