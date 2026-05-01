'use strict';

/**
 * adyenWebhookController.js
 *
 * Handles Adyen webhook ingestion, analytics, RCA enrichment, and event replay.
 *
 * Design principles (mirrors webhookController.js):
 *  - AUTHORISATION is the single source of truth for payment failures.
 *  - OFFER_CLOSED / ORDER_CLOSED are logged as IGNORED to prevent double-counting.
 *  - All DB calls are parameterised; no raw string interpolation for user input.
 *  - Retry posture is derived from Adyen's refusalReasonCode, never guessed.
 *  - Every public function validates its inputs before touching the DB.
 *
 * Adyen notification structure:
 *  POST body: { live: "false"|"true", notificationItems: [{ NotificationRequestItem }] }
 *  Each item: { eventCode, success, pspReference, merchantReference, reason,
 *               additionalData, amount: { value, currency }, eventDate }
 *
 * Signature verification is handled upstream by adyenWebhookMiddleware.
 */

const { logWebhook }    = require('../models/webhookModels');
const db                = require('../config/db');
const axios             = require('axios');
const { scheduleRetry } = require('../queues/retryQueue');
const { checkAndNotify} = require('../services/notificationService');

// ─── Constants ────────────────────────────────────────────────────────────────

/**
 * Maps Adyen refusalReasonCode → plain-English advice.
 * Adyen docs: https://docs.adyen.com/development-resources/refusal-reasons
 */
const RCA_ADVISOR = Object.freeze({
    '2':  'Refused by the issuing bank without a specific reason. Ask the customer to contact their bank or try a different card.',
    '3':  'Referral — the bank wants a voice authorisation. Ask the customer to use a different card.',
    '4':  'Acquirer error — a temporary issue on the acquirer side. Safe to retry once.',
    '5':  'Blocked card — the card is blocked by the issuer. Ask the customer to use a different card.',
    '6':  'Card expired. Ask the customer to update their payment method.',
    '7':  'Invalid card number entered. Ask the customer to re-enter their details.',
    '8':  'Invalid card PIN. Ask the customer to re-enter their PIN.',
    '9':  'Bank not available — temporary connectivity issue. Safe to retry.',
    '10': 'Not supported — this card type is not supported. Try a different payment method.',
    '11': '3D Secure authentication failed or not completed. Ask the customer to retry with 3DS.',
    '12': 'Not enough balance on the card. Ask the customer to top up or use a different card.',
    '14': 'Acquirer fraud — transaction flagged as fraudulent. Do NOT retry.',
    '15': 'Cancelled — customer or merchant cancelled the transaction.',
    '16': 'Shopper cancelled the 3D Secure flow. Ask the customer to retry.',
    '17': 'Invalid PIN entered too many times. Card is now blocked; customer must contact their bank.',
    '18': 'PIN tries exceeded — card blocked. Customer must contact their bank.',
    '19': 'Invalid response from the card. Ask the customer to retry or use a different card.',
    '20': 'Fraud — Adyen risk engine blocked this transaction. Do NOT retry — chargeback risk.',
    '21': 'Not Submitted — transaction never sent to the acquirer due to a config issue. Check your Adyen merchant account settings.',
    '22': 'Not enough balance (prepaid). Customer should top up or use a different card.',
    '23': 'Transaction not permitted — this transaction type is not allowed on this card.',
    '24': 'CVC declined — CVV mismatch. Ask the customer to re-enter card details carefully.',
    '25': 'Restricted card — the card has a restriction. Customer must contact their bank.',
    '26': 'Revocation of authorisation — standing order cancelled by the customer.',
    '27': 'Declined non-generic — check additionalData for specific decline details.',
    '28': 'Withdrawal count exceeded — too many withdrawals on the card.',
    '29': 'Withdrawal amount exceeded — withdrawal limit on the card reached.',
    '31': 'Fraud — issuer fraud block. Do NOT retry.',
    '32': 'AVS address mismatch. Ask the customer to re-enter their billing address.',
    '33': 'Card online PIN required — transaction requires online PIN.',
    '34': 'No checking account available — the card does not support this transaction type.',
    '35': 'No savings account available — the card does not support this transaction type.',
    '36': 'Mobile PIN required — transaction requires mobile PIN entry.',
    '37': 'Contactless fallback — transaction should be retried by inserting the card.',
    '38': 'Authentication required by issuer. Retry with 3DS enabled.',
    '39': 'RReq not received from DS — 3D Secure challenge failed silently. Retry with a fresh session.',
});

/**
 * Hard decline codes that must never be retried.
 * Codes 14, 20, 31 = fraud signals.
 */
const HARD_DECLINE_CODES = new Set(['14', '20', '31', '5', '17', '18']);

/** Valid analytics filter keys → their SQL config. */
const ANALYTICS_INTERVALS = Object.freeze({
    '1h':  { gap: '1 minute', range: '1 hour',  trunc: 'minute', format: 'HH24:MI'   },
    '24h': { gap: '1 hour',   range: '24 hours', trunc: 'hour',   format: 'HH24:00'  },
    '7d':  { gap: '1 day',    range: '7 days',   trunc: 'day',    format: 'DD Mon'   },
    '30d': { gap: '1 day',    range: '30 days',  trunc: 'day',    format: 'DD Mon'   },
    '1y':  { gap: '1 month',  range: '1 year',   trunc: 'month',  format: 'Mon YYYY' },
});

const DEFAULT_FILTER    = '24h';
const REPLAY_TIMEOUT_MS = 5_000;

/**
 * Adyen event codes that are the canonical failure / success signals.
 * OFFER_CLOSED / ORDER_CLOSED mirror payment_intent.payment_failed on Stripe —
 * they fire alongside AUTHORISATION and are logged as IGNORED.
 */
const IGNORED_EVENT_CODES = new Set(['OFFER_CLOSED', 'ORDER_CLOSED']);

/**
 * Adyen event codes surfaced in the recent activity feed (mirrors Stripe set).
 */
const ACTIVITY_FEED_EVENTS = new Set([
    'AUTHORISATION',
    'CANCELLATION',
    'REFUND',
    'CAPTURE',
    'CHARGEBACK',
    'CHARGEBACK_REVERSED',
    'RECURRING_CONTRACT',
    'REPORT_AVAILABLE',
]);

// ─── Pure helpers ─────────────────────────────────────────────────────────────

/**
 * Derives retry posture from Adyen's refusalReasonCode.
 *
 * @param {string|null} refusalReasonCode
 * @returns {'DO_NOT_RETRY'|'RETRY'|'RETRY_ONCE'}
 */
const getRetryPosture = (refusalReasonCode) => {
    if (!refusalReasonCode) return 'RETRY_ONCE';
    if (HARD_DECLINE_CODES.has(String(refusalReasonCode))) return 'DO_NOT_RETRY';

    // Transient acquirer / network codes — safe to retry with backoff
    if (['4', '9'].includes(String(refusalReasonCode))) return 'RETRY';

    return 'RETRY_ONCE';
};

/**
 * Returns a human-readable RCA hint, falling back gracefully.
 *
 * @param {string|null} refusalReasonCode
 * @param {string|null} reason  - raw refusal reason string from Adyen
 * @returns {string}
 */
const getRcaHint = (refusalReasonCode, reason) =>
    RCA_ADVISOR[String(refusalReasonCode)]
    ?? reason
    ?? 'Unknown decline — check the payload for details.';

/**
 * Extracts AUTHORISATION-failure metadata from an Adyen NotificationRequestItem.
 *
 * @param {object} item - NotificationRequestItem
 * @returns {{ refusalReasonCode, reason, errorMessage, errorStack, retryPosture }}
 */
const extractAuthorisationFailedMeta = (item) => {
    const additionalData   = item.additionalData ?? {};
    const refusalReasonCode = additionalData.refusalReasonCode ?? null;
    const reason           = item.reason ?? additionalData.refusalReason ?? null;

    const retryPosture = getRetryPosture(refusalReasonCode);

    const errorMessage = refusalReasonCode ?? reason ?? 'AUTHORISATION_FAILED';
    const errorStack   = JSON.stringify({
        refusal_reason_code: refusalReasonCode,
        refusal_reason:      reason,
        retry_posture:       retryPosture,
        auth_code:           additionalData.authCode            ?? null,
        avs_result:          additionalData.avsResultRaw        ?? null,
        cvc_result:          additionalData.cvcResultRaw        ?? null,
        risk_score:          additionalData.riskScore           ?? null,
        psp_reference:       item.pspReference                  ?? null,
        merchant_reference:  item.merchantReference             ?? null,
        rca_hint:            getRcaHint(refusalReasonCode, reason),
    });

    return { refusalReasonCode, reason, errorMessage, errorStack, retryPosture };
};

// ─── Webhook ingestion ────────────────────────────────────────────────────────

/**
 * POST /webhooks/adyen/:gateway_id
 *
 * Ingests a verified Adyen notification batch.
 * Adyen sends notifications as an array — each item is processed individually.
 * AUTHORISATION with success=false is the canonical failure signal.
 * OFFER_CLOSED / ORDER_CLOSED are logged as IGNORED to avoid double-counting.
 *
 * Adyen requires the response body to be exactly: [accepted]
 */
const handleAdyenWebhook = async (req, res) => {
    const io              = req.app.get('socketio');
    const { gateway_id }  = req.params;
    // Middleware has already verified the HMAC and attached notificationItems
    const items           = req.adyenNotifications; // [NotificationRequestItem]
    const startTime       = Date.now();

    try {
        // Process each notification item independently — Adyen batches multiple
        // items per HTTP call but each has its own pspReference and eventCode.
        await Promise.all(items.map(item => processAdyenItem(item, gateway_id, io, startTime)));

        // Adyen requires this exact acknowledgement body
        return res.status(200).send('[accepted]');

    } catch (err) {
        console.error('🔥 Adyen webhook controller error:', err.message);

        // Best-effort error log for the batch
        await logWebhook({
            gateway_id,
            provider_event_id:  `adyen_batch_${Date.now()}`,
            event_type:         'BATCH_ERROR',
            status:             'FAILED',
            http_status_code:   500,
            payload:            req.body ?? {},
            error_stack:        `System Error: ${err.message}`,
            last_error_message: 'internal_server_error',
            latency:            Date.now() - startTime,
        }).catch(() => {});

        // Still return [accepted] — Adyen will otherwise retry the entire batch
        return res.status(200).send('[accepted]');
    }
};

/**
 * Processes a single Adyen NotificationRequestItem.
 * Extracted so handleAdyenWebhook can Promise.all over batches.
 *
 * @param {object} item        - NotificationRequestItem
 * @param {string} gateway_id
 * @param {object|null} io     - Socket.IO instance
 * @param {number} startTime   - batch start timestamp for latency
 */
const processAdyenItem = async (item, gateway_id, io, startTime) => {
    const eventCode = item.eventCode;
    const success   = item.success === 'true' || item.success === true;
    const amount    = item.amount?.value ?? null;          // in minor units (cents)

    let status       = 'SUCCESS';
    let errorMessage = null;
    let errorStack   = null;

    // ── 1. IGNORED events ─────────────────────────────────────────────
    // OFFER_CLOSED / ORDER_CLOSED fire alongside AUTHORISATION for the
    // same transaction — log for audit, mark IGNORED to avoid double-count.
    if (IGNORED_EVENT_CODES.has(eventCode)) {
        await logWebhook({
            gateway_id,
            provider_event_id:  item.pspReference,
            event_type:         eventCode,
            status:             'IGNORED',
            http_status_code:   200,
            payload:            item,
            error_stack:        null,
            last_error_message: null,
            latency:            Date.now() - startTime,
        });
        emitSocketEvent(io, gateway_id, eventCode, 'IGNORED');
        return;
    }

    // ── 2. AUTHORISATION failure ───────────────────────────────────────
    // Single source of truth for Adyen payment failures.
    if (eventCode === 'AUTHORISATION' && !success) {
        status = 'FAILED';
        const meta = extractAuthorisationFailedMeta(item);
        errorMessage = meta.errorMessage;
        errorStack   = meta.errorStack;
    }

    // ── 3. Chargeback / dispute events ────────────────────────────────
    else if (eventCode === 'CHARGEBACK' || eventCode === 'SECOND_CHARGEBACK') {
        status       = 'CRITICAL';
        errorMessage = 'dispute_initiated';
        errorStack   = JSON.stringify({
            rca_hint:          'A chargeback has been raised. Defend the dispute in your Adyen Customer Area before the deadline.',
            psp_reference:     item.pspReference     ?? null,
            merchant_reference: item.merchantReference ?? null,
            amount:            amount,
            currency:          item.amount?.currency  ?? null,
        });
    }

    // ── 4. All other events (successful AUTH, CAPTURE, REFUND…) ───────
    // status = 'SUCCESS', errorMessage/errorStack remain null.

    const latency = Date.now() - startTime;

    await logWebhook({
        gateway_id,
        provider_event_id:  item.pspReference,
        event_type:         eventCode,
        status,
        http_status_code:   200,
        payload:            item,
        error_stack:        errorStack,
        last_error_message: errorMessage,
        latency,
    });

    // ── 5. Schedule auto-retry for retryable AUTHORISATION failures ───
    if (eventCode === 'AUTHORISATION' && status === 'FAILED') {
        const additionalData    = item.additionalData ?? {};
        const refusalReasonCode = additionalData.refusalReasonCode ?? null;
        const retryPosture      = getRetryPosture(refusalReasonCode);

        if (retryPosture !== 'DO_NOT_RETRY') {
            await scheduleAutoRetryIfConfigured(item, gateway_id, retryPosture);
        }
    }

    // ── 6. Threshold notifications ────────────────────────────────────
    if (status === 'FAILED' || status === 'CRITICAL') {
        checkAndNotify({ gateway_id }).catch((err) =>
            console.error('❌ Adyen notification error:', err.message),
        );
    }

    emitSocketEvent(io, gateway_id, eventCode, status);
};

// ─── Analytics ────────────────────────────────────────────────────────────────

/**
 * GET /gateways/adyen/:gateway_id/analytics?filter=24h
 *
 * Returns summary stats, chart data, top errors, recent events, and
 * loss/recovery figures for an Adyen gateway in the given time window.
 *
 * Adyen amounts are in minor units (e.g. 1000 = $10.00) — divided by 100.
 * All SQL intervals are whitelisted against ANALYTICS_INTERVALS.
 */
const getGatewayAnalytics = async (req, res) => {
    const { gateway_id } = req.params;
    const filterKey      = ANALYTICS_INTERVALS[req.query.filter] ? req.query.filter : DEFAULT_FILTER;
    const cfg            = ANALYTICS_INTERVALS[filterKey];
    const { gap, range, trunc, format } = cfg;

    try {
        const [
            isActiveRes,
            summaryRes,
            chartRes,
            errorsRes,
            recentHitsRes,
            paymentLossRes,
            deliveryLossRes,
            recoveryRes,
        ] = await Promise.all([

            db.query(
                `SELECT is_active FROM gateways WHERE id = $1`,
                [gateway_id],
            ),

            // 1. Summary — excludes IGNORED rows
            db.query(
                `SELECT
                    COUNT(*)  FILTER (WHERE status != 'IGNORED')              AS total_count,
                    COUNT(*)  FILTER (WHERE status = 'SUCCESS')               AS success_count,
                    COUNT(*)  FILTER (WHERE status IN ('FAILED','CRITICAL'))  AS failure_count
                 FROM webhook_logs
                 WHERE gateway_id   = $1
                   AND provider_type = 'ADYEN'
                   AND received_at  >= NOW() - INTERVAL '${range}'`,
                [gateway_id],
            ),

            // 2. Time-series chart
            db.query(
                `SELECT
                    to_char(series.time_slot, '${format}')                                          AS label,
                    COALESCE(COUNT(w.id) FILTER (WHERE w.status = 'SUCCESS'), 0)                    AS success,
                    COALESCE(COUNT(w.id) FILTER (WHERE w.status IN ('FAILED','CRITICAL')), 0)       AS failed
                 FROM generate_series(
                     date_trunc('${trunc}', NOW() - INTERVAL '${range}') + INTERVAL '${gap}',
                     date_trunc('${trunc}', NOW()),
                     '${gap}'::interval
                 ) AS series(time_slot)
                 LEFT JOIN webhook_logs w
                     ON date_trunc('${trunc}', w.received_at) = series.time_slot
                    AND w.gateway_id    = $1
                    AND w.provider_type = 'ADYEN'
                    AND w.status       != 'IGNORED'
                 GROUP BY series.time_slot
                 ORDER BY series.time_slot ASC`,
                [gateway_id],
            ),

            // 3. Top 5 error codes for RCA table
            db.query(
                `SELECT
                    last_error_message,
                    COUNT(*) AS occurrence
                 FROM webhook_logs
                 WHERE gateway_id    = $1
                   AND provider_type = 'ADYEN'
                   AND status       IN ('FAILED','CRITICAL')
                   AND received_at  >= NOW() - INTERVAL '${range}'
                   AND last_error_message IS NOT NULL
                   AND last_error_message != ''
                 GROUP BY last_error_message
                 ORDER BY occurrence DESC
                 LIMIT 5`,
                [gateway_id],
            ),

            // 4. Recent activity feed
            // Adyen stores amounts in minor units inside payload->amount->value
            db.query(
                `SELECT
                    id,
                    event_type,
                    status,
                    to_char(received_at, 'HH24:MI:SS')                             AS time,
                    CASE
                        WHEN (payload->>'amount') IS NOT NULL
                        THEN ((payload->'amount')->>'value')::numeric / 100
                    END                                                             AS amount,
                    (payload->'amount')->>'currency'                                AS currency,
                    payload->>'pspReference'                                        AS psp_reference,
                    payload->>'merchantReference'                                   AS merchant_reference,
                    payload->>'reason'                                              AS decline_reason,
                    (payload->'additionalData')->>'refusalReasonCode'               AS refusal_reason_code,
                    (payload->'additionalData')->>'cardSummary'                     AS card_last4,
                    (payload->'additionalData')->>'paymentMethod'                   AS card_brand
                 FROM webhook_logs
                 WHERE gateway_id    = $1
                   AND provider_type = 'ADYEN'
                   AND status       != 'IGNORED'
                   AND event_type   IN (
                       'AUTHORISATION',
                       'CANCELLATION',
                       'REFUND',
                       'CAPTURE',
                       'CHARGEBACK',
                       'CHARGEBACK_REVERSED',
                       'RECURRING_CONTRACT'
                   )
                 ORDER BY received_at DESC
                 LIMIT 10`,
                [gateway_id],
            ),

            // 5. Payment loss — failed AUTHORISATION events only
            db.query(
                `SELECT
                    COALESCE(
                        SUM(((payload->'amount')->>'value')::numeric / 100), 0
                    ) AS total_payment_loss,
                    COUNT(*) AS failed_charge_count
                 FROM webhook_logs
                 WHERE gateway_id    = $1
                   AND provider_type = 'ADYEN'
                   AND event_type   = 'AUTHORISATION'
                   AND status       = 'FAILED'
                   AND received_at  >= NOW() - INTERVAL '${range}'
                   AND (payload->'amount')->>'value' IS NOT NULL`,
                [gateway_id],
            ),

            // 6. Delivery failure loss — infrastructure errors
            db.query(
                `SELECT
                    COALESCE(
                        SUM(((payload->'amount')->>'value')::numeric / 100), 0
                    ) AS total_delivery_loss,
                    COUNT(*) AS delivery_failure_count
                 FROM webhook_logs
                 WHERE gateway_id    = $1
                   AND provider_type = 'ADYEN'
                   AND status       = 'FAILED'
                   AND event_type  != 'AUTHORISATION'
                   AND received_at  >= NOW() - INTERVAL '${range}'
                   AND (
                       error_stack     ILIKE '%timeout%'
                    OR error_stack     ILIKE '%connection%'
                    OR error_stack     ILIKE '%ECONNREFUSED%'
                    OR http_status_code = 500
                   )`,
                [gateway_id],
            ),

            // 7. Recovery — successful auto / manual retries
            db.query(
                `SELECT
                    COALESCE(SUM(amount), 0)                                          AS total_recovered,
                    COUNT(*)                                                           AS recovery_count,
                    COUNT(*) FILTER (WHERE recovery_type = 'AUTO_RETRY')              AS auto_retry_count,
                    COUNT(*) FILTER (WHERE recovery_type = 'MANUAL_RETRY')            AS manual_retry_count
                 FROM recovery_logs
                 WHERE gateway_id   = $1
                   AND recovered_at >= NOW() - INTERVAL '${range}'`,
                [gateway_id],
            ),
        ]);

        const isActive = isActiveRes.rows[0]?.is_active ?? false;
        const stats    = summaryRes.rows[0];
        const total    = parseInt(stats.total_count,   10);
        const success  = parseInt(stats.success_count, 10);
        const failure  = parseInt(stats.failure_count, 10);

        const successRate    = total > 0 ? Math.round((success / total) * 100) : 0;
        const paymentLoss    = parseFloat(paymentLossRes.rows[0].total_payment_loss   ?? 0);
        const deliveryLoss   = parseFloat(deliveryLossRes.rows[0].total_delivery_loss ?? 0);
        const totalLoss      = paymentLoss + deliveryLoss;
        const totalRecovered = parseFloat(recoveryRes.rows[0].total_recovered         ?? 0);
        const netLoss        = Math.max(0, totalLoss - totalRecovered);

        return res.status(200).json({
            gateway_id,
            provider:  'ADYEN',
            is_active: isActive,
            filter:    filterKey,
            summary: {
                total_events:   total,
                success_events: success,
                failed_events:  failure,
                success_rate:   `${successRate}%`,
            },
            loss: {
                payment_loss: {
                    amount:       formatUsd(paymentLoss),
                    raw:          paymentLoss,
                    charge_count: parseInt(paymentLossRes.rows[0].failed_charge_count ?? 0, 10),
                },
                delivery_loss: {
                    amount:        formatUsd(deliveryLoss),
                    raw:           deliveryLoss,
                    failure_count: parseInt(deliveryLossRes.rows[0].delivery_failure_count ?? 0, 10),
                },
                total_at_risk: {
                    amount: formatUsd(totalLoss),
                    raw:    totalLoss,
                },
                recovered: {
                    amount:             formatUsd(totalRecovered),
                    raw:                totalRecovered,
                    recovery_count:     parseInt(recoveryRes.rows[0].recovery_count      ?? 0, 10),
                    auto_retry_count:   parseInt(recoveryRes.rows[0].auto_retry_count    ?? 0, 10),
                    manual_retry_count: parseInt(recoveryRes.rows[0].manual_retry_count  ?? 0, 10),
                },
                net_loss: {
                    amount: formatUsd(netLoss),
                    raw:    netLoss,
                },
            },
            top_errors:  errorsRes.rows,
            chart_data:  chartRes.rows,
            recent_hits: recentHitsRes.rows,
        });

    } catch (err) {
        console.error('❌ Adyen analytics error:', err.message);
        return res.status(500).json({ error: 'Analytics query failed.' });
    }
};

// ─── Webhook detail + RCA ─────────────────────────────────────────────────────

/**
 * GET /webhooks/adyen/:webhook_id
 *
 * Returns a single Adyen webhook log enriched with:
 *  - RCA rule match (rca_rules table via LATERAL join — same table as Stripe)
 *  - Retry posture derived from refusalReasonCode
 *  - Failure classification (USER_FAILURE vs SYSTEM_FAILURE)
 *  - Plain-English RCA hint (rca_rules or local RCA_ADVISOR fallback)
 *
 * The rca_rules table uses error_code to match Adyen's refusalReasonCode,
 * and error_pattern for ILIKE fallbacks on last_error_message.
 */
const getWebhookDetails = async (req, res) => {
    const { webhook_id } = req.params;

    try {
        const { rows } = await db.query(
            `SELECT
                w.*,
                r.issue_category,
                r.suggested_fix,
                r.severity AS rca_severity,

                -- Retry posture — meaningful for failed AUTHORISATION only
                CASE
                    WHEN w.event_type = 'AUTHORISATION' AND w.status = 'FAILED' THEN
                        CASE
                            WHEN (w.payload->'additionalData')->>'refusalReasonCode'
                                 IN ('14','20','31','5','17','18')
                            THEN 'DO_NOT_RETRY'
                            WHEN (w.payload->'additionalData')->>'refusalReasonCode'
                                 IN ('4','9')
                            THEN 'RETRY'
                            ELSE 'RETRY_ONCE'
                        END
                END AS retry_posture,

                -- Failure classification
                CASE
                    WHEN w.event_type = 'AUTHORISATION' AND w.status = 'FAILED'
                                                           THEN 'USER_FAILURE'
                    WHEN w.error_stack ILIKE ANY(ARRAY[
                             '%timeout%','%ECONNREFUSED%','%500%'
                         ])                                THEN 'SYSTEM_FAILURE'
                    ELSE 'UNKNOWN'
                END AS failure_type

             FROM webhook_logs w
             LEFT JOIN LATERAL (
                 SELECT *
                 FROM rca_rules r
                 WHERE
                     -- Match Adyen refusalReasonCode against error_code column
                     (r.error_code IS NOT NULL
                      AND (w.payload->'additionalData')->>'refusalReasonCode' = r.error_code)
                  OR -- Match on pattern fallback
                     (r.error_pattern IS NOT NULL
                      AND w.last_error_message ILIKE '%' || r.error_pattern || '%')
                 ORDER BY
                     CASE
                         WHEN r.error_code IS NOT NULL
                              AND (w.payload->'additionalData')->>'refusalReasonCode' = r.error_code
                         THEN 2
                         ELSE 1
                     END DESC,
                     r.id DESC
                 LIMIT 1
             ) r ON true
             WHERE w.id = $1`,
            [webhook_id],
        );

        if (rows.length === 0) {
            return res.status(404).json({ error: 'Webhook not found.' });
        }

        const row               = rows[0];
        const refusalReasonCode = row.payload?.additionalData?.refusalReasonCode ?? null;
        const reason            = row.payload?.reason ?? null;

        const rcaHint = row.suggested_fix
                     ?? getRcaHint(refusalReasonCode, reason);

        return res.status(200).json({ ...row, rca_hint: rcaHint });

    } catch (err) {
        console.error('❌ Adyen RCA engine error:', err.message);
        return res.status(500).json({ error: 'Failed to run RCA analysis.' });
    }
};

// ─── Replay ───────────────────────────────────────────────────────────────────

/**
 * POST /webhooks/adyen/:id/replay
 *
 * Re-delivers a stored Adyen notification payload to the gateway's target URL.
 * Hard fraud declines (DO_NOT_RETRY) and CHARGEBACK events are blocked.
 */
const replayWebhook = async (req, res) => {
    const { id } = req.params;

    try {
        const { rows } = await db.query(
            `SELECT wl.*, g.webhook_secret, g.webhook_url
             FROM webhook_logs wl
             JOIN gateways g ON wl.gateway_id = g.id
             WHERE wl.id = $1`,
            [id],
        );

        if (rows.length === 0) {
            return res.status(404).json({ error: 'Webhook not found.' });
        }

        const webhook = rows[0];

        // Guard: block chargebacks — replaying disputes is meaningless
        if (webhook.event_type === 'CHARGEBACK' || webhook.event_type === 'SECOND_CHARGEBACK') {
            return res.status(400).json({
                error:  'Replay blocked.',
                reason: 'Chargeback events cannot be replayed — manage the dispute in your Adyen Customer Area.',
            });
        }

        // Guard: block hard fraud declines
        const refusalReasonCode = webhook.payload?.additionalData?.refusalReasonCode ?? null;
        const retryPosture      = getRetryPosture(refusalReasonCode);

        if (webhook.event_type === 'AUTHORISATION'
            && webhook.status === 'FAILED'
            && retryPosture === 'DO_NOT_RETRY') {
            return res.status(400).json({
                error:               'Replay blocked.',
                reason:              'Hard decline — replaying will yield the same result.',
                retry_posture:       'DO_NOT_RETRY',
                refusal_reason_code: refusalReasonCode,
            });
        }

        if (!webhook.webhook_url) {
            return res.status(400).json({ error: 'No target URL configured for this gateway.' });
        }

        // ── Deliver ────────────────────────────────────────────────────
        const start = Date.now();
        let httpResponse = null;
        let replayStatus = 'SUCCESS';
        let errorStack   = null;

        try {
            httpResponse = await axios.post(
                webhook.webhook_url,
                // Adyen expects the batch envelope format
                { live: 'false', notificationItems: [{ NotificationRequestItem: webhook.payload }] },
                {
                    headers: {
                        'Content-Type': 'application/json',
                        ...(webhook.request_headers ?? {}),
                    },
                    timeout: REPLAY_TIMEOUT_MS,
                },
            );
        } catch (err) {
            replayStatus = 'FAILED';
            errorStack   = err.message;
        }

        const latency = Date.now() - start;

        // ── Persist replay log ─────────────────────────────────────────
        await db.query(
            `INSERT INTO webhook_logs (
                gateway_id, provider_event_id, event_type, status,
                http_status_code, latency_ms, payload, request_headers,
                error_stack, last_error_message, retry_count, replayed_from,
                provider_type
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'ADYEN')`,
            [
                webhook.gateway_id,
                `${webhook.provider_event_id}_replay_${Date.now()}`,
                webhook.event_type,
                replayStatus,
                httpResponse?.status ?? 500,
                latency,
                webhook.payload,
                webhook.request_headers,
                errorStack,
                errorStack,
                (webhook.retry_count ?? 0) + 1,
                webhook.id,
            ],
        );

        return res.status(200).json({
            message:       'Webhook replayed.',
            status:        replayStatus,
            latency_ms:    latency,
            retry_posture: retryPosture,
        });

    } catch (err) {
        console.error('❌ Adyen replay error:', err.message);
        return res.status(500).json({ error: 'Replay failed.' });
    }
};

// ─── Private helpers ──────────────────────────────────────────────────────────

const emitSocketEvent = (io, gatewayId, eventType, status) => {
    if (!io) return;
    io.emit(`update_dashboard_${gatewayId}`, {
        event:     eventType,
        status,
        timestamp: new Date(),
    });
};

const scheduleAutoRetryIfConfigured = async (item, gatewayId, retryPosture) => {
    try {
        const [gatewayRes, logRes] = await Promise.all([
            db.query('SELECT webhook_url FROM gateways WHERE id = $1', [gatewayId]),
            db.query(
                `SELECT id FROM webhook_logs
                 WHERE provider_event_id = $1 AND gateway_id = $2
                 ORDER BY received_at DESC LIMIT 1`,
                [item.pspReference, gatewayId],
            ),
        ]);

        const targetUrl    = gatewayRes.rows[0]?.webhook_url;
        const webhookLogId = logRes.rows[0]?.id;

        if (!targetUrl || !webhookLogId) return;

        await scheduleRetry({
            webhookLogId,
            gatewayId,
            payload:       item,
            targetUrl,
            attemptNumber: 1,
            retryPosture,
        });
    } catch (err) {
        console.error('⚠️  Adyen auto-retry scheduling failed (non-fatal):', err.message);
    }
};

const formatUsd = (value) => `$${value.toFixed(2)}`;

// ─── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
    handleAdyenWebhook,
    getGatewayAnalytics,
    getWebhookDetails,
    replayWebhook,
};