'use strict';

/**
 * paypalWebhookController.js
 *
 * Handles PayPal webhook ingestion, analytics, RCA enrichment, and event replay.
 *
 * Design principles (mirrors webhookController.js):
 *  - PAYMENT.CAPTURE.DENIED is the single source of truth for payment failures.
 *  - PAYMENT.ORDER.CANCELLED is logged as IGNORED to prevent double-counting.
 *  - All DB calls are parameterised; no raw string interpolation for user input.
 *  - Retry posture is derived from PayPal's processor_response codes, never guessed.
 *  - Every public function validates its inputs before touching the DB.
 *
 * PayPal webhook structure:
 *  POST body: { id, event_type, event_version, resource_type, resource,
 *               summary, create_time, links }
 *  resource:  Capture | Authorization | Order | Dispute object
 *
 * Signature verification is handled upstream by paypalWebhookMiddleware,
 * which calls the PayPal /v1/notifications/verify-webhook-signature endpoint.
 */

const { logWebhook }    = require('../models/webhookModels');
const db                = require('../config/db');
const axios             = require('axios');
const { scheduleRetry } = require('../queues/retryQueue');
const { checkAndNotify} = require('../services/notificationService');

// ─── Constants ────────────────────────────────────────────────────────────────

/**
 * Maps PayPal processor_response_code → plain-English advice.
 * Codes are returned in resource.processor_response.response_code.
 * PayPal docs: https://developer.paypal.com/api/rest/reference/orders/v2/errors/
 */
const RCA_ADVISOR = Object.freeze({
    // ── Generic bank responses ────────────────────────────────────────────────
    '0000': 'Approved. No action needed.',
    '0100': 'Referral — bank wants voice authorisation. Ask customer to use a different card.',
    '0200': 'Do Not Honor — hard bank decline. Ask customer to contact their bank or use a different card.',
    '0300': 'Invalid merchant account setup. Check your PayPal merchant configuration.',
    '0500': 'Do Not Honor — same as 0200. Customer must contact their bank.',
    '0580': 'Blocked — transaction blocked by the bank. Do not retry.',
    '0800': 'Bank network error. Safe to retry once after a short delay.',
    '0880': 'Cryptographic failure. Check your integration — this should not happen in production.',
    '0R00': 'Revocation of an authorisation order. Customer cancelled a recurring payment; update records.',
    '1000': 'Partial approval — the full amount was not approved. Handle partial captures or request full amount separately.',
    '1300': 'Invalid account number / card number. Ask customer to re-enter their card details.',
    '1310': 'Invalid amount. Check the transaction amount in your integration.',
    '1312': 'Invalid card type for this transaction. Ask customer to use a different card.',
    '1317': 'Card expired. Ask customer to update their payment method.',
    '1320': 'Invalid CVV. Ask customer to re-enter their card details carefully.',
    '1330': 'Card not active. Ask customer to contact their bank.',
    '1335': 'Card not enabled for internet purchases. Ask customer to enable online transactions or use a different card.',
    '1340': 'Card restriction. Ask customer to contact their bank.',
    '1350': 'Cardholder not enrolled in credit programme. Use a different payment method.',
    '1352': 'Card not enrolled in Verified by Visa / 3DS. Consider disabling mandatory 3DS for this card.',
    '1360': 'Card not eligible for this type of transaction.',
    '1370': 'Invalid account type. Ask customer to use a different card.',
    '1380': 'Expired card record at the bank.',
    '1390': 'Card not eligible for this amount range. Try a smaller amount.',
    '1500': 'Insufficient funds. Send a payment recovery email. Do not auto-retry — wait 48–72h.',
    '1510': 'Insufficient funds in the account.',
    '1520': 'Credit limit exceeded. Ask customer to use a different card.',
    '1600': 'Transaction amount exceeds the approved limit.',
    '1601': 'Exceeds frequency limit. Too many transactions on this card.',
    '5100': 'Generic decline — bank declined without a specific reason. Retry once after 24h or ask customer to try a different card.',
    '5110': 'CVV mismatch. Ask customer to re-enter card details carefully.',
    '5120': 'Insufficient funds (variant). Wait 48–72h before retrying.',
    '5130': 'Invalid PIN entered.',
    '5135': 'Decline — surcharge amount not permitted.',
    '5140': 'Card closed.',
    '5150': 'Too many PIN tries. Card is now blocked; customer must contact bank.',
    '5160': 'Decline — lost or stolen card. Do not retry — flag for review.',
    '5170': 'Decline — fraudulent card. Do NOT retry — chargeback risk.',
    '5180': 'Decline — unknown card. Ask customer to check card number.',
    '5190': 'Declined — bank declined without further details.',
    '5200': 'Ineligible for given transaction type. Use a different payment method.',
    '5400': 'Expired card. Ask customer to update their payment method.',
    '5500': 'Incorrect PIN. Ask customer to re-enter PIN or use chip-and-PIN.',
    '5650': 'Decline — do not retry.',
    '5700': 'Card velocity limit exceeded. Pause retries for 24h.',
    '5800': 'Reversal rejected.',
    '5900': 'Invalid transaction.',
    '5910': 'Auth not found.',
    '5920': 'Auth expired.',
    '5930': 'Card not active.',
    '6300': 'Dispute — the transaction is in a dispute state. Check your PayPal resolution centre.',
    '9100': 'Declined — contact the card issuer.',
    '9500': 'Potential fraud detected. Do NOT retry — review and flag the account.',
    '9510': 'Velocity exceeded. Too many attempts. Pause and retry after 24h.',
    '9520': 'High fraud risk. Do NOT retry.',
    '9530': 'Suspected fraud. Flag the account for manual review.',
    '9540': 'Frozen card. Customer must contact their bank.',
    '9600': 'Soft decline. Retry once after a short delay.',
    'PPMD': 'PayPal Merchant declined. Check your PayPal account for restrictions.',
    'PPAE': 'PayPal — this transaction exceeded the maximum amount allowed.',
    'PPAD': 'Declined due to PayPal policy. Check your account settings.',
});

/**
 * Hard decline codes that must never be retried.
 */
const HARD_DECLINE_CODES = new Set([
    '0200', '0500', '0580', '5160', '5170',
    '5650', '9500', '9520', '9530', 'PPAD',
]);

/**
 * PayPal event_type values that are IGNORED (duplicates of the canonical failure).
 * PAYMENT.ORDER.CANCELLED mirrors payment_intent.payment_failed — it fires
 * alongside PAYMENT.CAPTURE.DENIED for the same underlying transaction.
 */
const IGNORED_EVENT_TYPES = new Set([
    'PAYMENT.ORDER.CANCELLED',
    'CHECKOUT.ORDER.DECLINED',   // older API — same pattern
]);

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

// ─── Pure helpers ─────────────────────────────────────────────────────────────

/**
 * Derives retry posture from PayPal's processor_response_code.
 *
 * @param {string|null} responseCode - processor_response.response_code
 * @returns {'DO_NOT_RETRY'|'RETRY'|'RETRY_ONCE'}
 */
const getRetryPosture = (responseCode) => {
    if (!responseCode) return 'RETRY_ONCE';
    if (HARD_DECLINE_CODES.has(responseCode)) return 'DO_NOT_RETRY';

    // Transient / network errors — safe to retry with backoff
    if (['0800', '9600'].includes(responseCode)) return 'RETRY';

    return 'RETRY_ONCE';
};

/**
 * Returns a human-readable RCA hint, falling back gracefully.
 *
 * @param {string|null} responseCode
 * @param {string|null} description - PayPal's own description field
 * @returns {string}
 */
const getRcaHint = (responseCode, description) =>
    RCA_ADVISOR[responseCode]
    ?? description
    ?? 'Unknown decline — check the payload for details.';

/**
 * Extracts PAYMENT.CAPTURE.DENIED metadata from a PayPal webhook resource.
 *
 * @param {object} resource - PayPal Capture object
 * @returns {{ responseCode, description, errorMessage, errorStack, retryPosture }}
 */
const extractCaptureDeniedMeta = (resource) => {
    const processorResponse = resource.processor_response ?? {};
    const responseCode      = processorResponse.response_code       ?? null;
    const avsCode           = processorResponse.avs_code            ?? null;
    const cvvCode           = processorResponse.cvv_code            ?? null;
    const description       = processorResponse.response_code_description ?? null;

    const retryPosture = getRetryPosture(responseCode);

    const errorMessage = responseCode ?? description ?? 'CAPTURE_DENIED';
    const errorStack   = JSON.stringify({
        response_code:       responseCode,
        response_description: description,
        avs_code:            avsCode,
        cvv_code:            cvvCode,
        retry_posture:       retryPosture,
        capture_id:          resource.id                              ?? null,
        order_id:            resource.supplementary_data?.related_ids?.order_id ?? null,
        amount:              resource.amount?.value                   ?? null,
        currency:            resource.amount?.currency_code           ?? null,
        rca_hint:            getRcaHint(responseCode, description),
    });

    return { responseCode, description, errorMessage, errorStack, retryPosture };
};

// ─── Webhook ingestion ────────────────────────────────────────────────────────

/**
 * POST /webhooks/paypal/:gateway_id
 *
 * Ingests a verified PayPal webhook event.
 * PAYMENT.CAPTURE.DENIED is the canonical failure signal.
 * PAYMENT.ORDER.CANCELLED is logged as IGNORED to avoid double-counting.
 *
 * PayPal requires HTTP 200 to acknowledge receipt; any other status triggers retries.
 */
const handlePaypalWebhook = async (req, res) => {
    const io             = req.app.get('socketio');
    const { gateway_id } = req.params;
    const event          = req.paypalEvent;   // verified and attached by middleware
    const startTime      = Date.now();

    let status       = 'SUCCESS';
    let errorMessage = null;
    let errorStack   = null;

    try {
        // ── 1. IGNORED events ──────────────────────────────────────────────────
        // PAYMENT.ORDER.CANCELLED fires alongside PAYMENT.CAPTURE.DENIED for the
        // same decline. Log for audit, mark IGNORED to prevent double-counting.
        if (IGNORED_EVENT_TYPES.has(event.event_type)) {
            await logWebhook({
                gateway_id,
                provider_event_id:  event.id,
                event_type:         event.event_type,
                status:             'IGNORED',
                http_status_code:   200,
                payload:            event,
                error_stack:        null,
                last_error_message: null,
                latency:            Date.now() - startTime,
            });
            emitSocketEvent(io, gateway_id, event.event_type, 'IGNORED');
            return res.status(200).json({
                received: true,
                note:     'Duplicate of PAYMENT.CAPTURE.DENIED — ignored for counting.',
            });
        }

        // ── 2. PAYMENT.CAPTURE.DENIED ──────────────────────────────────────────
        // Single source of truth for PayPal payment failures.
        if (event.event_type === 'PAYMENT.CAPTURE.DENIED') {
            status = 'FAILED';
            const meta = extractCaptureDeniedMeta(event.resource ?? {});
            errorMessage = meta.errorMessage;
            errorStack   = meta.errorStack;
        }

        // ── 3. PAYMENT.CAPTURE.REFUND-REVERSED (friendly fraud signal) ─────────
        else if (event.event_type === 'CUSTOMER.DISPUTE.CREATED'
              || event.event_type === 'CUSTOMER.DISPUTE.UPDATED') {
            status       = 'CRITICAL';
            errorMessage = 'dispute_initiated';
            errorStack   = JSON.stringify({
                rca_hint:    'A PayPal dispute has been opened. Respond in the PayPal Resolution Centre before the deadline.',
                dispute_id:  event.resource?.id                     ?? null,
                reason:      event.resource?.reason                 ?? null,
                status:      event.resource?.status                 ?? null,
                amount:      event.resource?.disputed_transactions?.[0]?.seller_transaction_id ?? null,
            });
        }

        // ── 4. All other events (PAYMENT.CAPTURE.COMPLETED, REFUND…) ──────────
        // status = 'SUCCESS', errorMessage/errorStack remain null.

        const latency = Date.now() - startTime;

        await logWebhook({
            gateway_id,
            provider_event_id:  event.id,
            event_type:         event.event_type,
            status,
            http_status_code:   200,
            payload:            event,
            error_stack:        errorStack,
            last_error_message: errorMessage,
            latency,
        });

        // ── 5. Schedule auto-retry for retryable capture failures ──────────────
        if (event.event_type === 'PAYMENT.CAPTURE.DENIED' && status === 'FAILED') {
            const responseCode = event.resource?.processor_response?.response_code ?? null;
            const retryPosture = getRetryPosture(responseCode);

            if (retryPosture !== 'DO_NOT_RETRY') {
                await scheduleAutoRetryIfConfigured(event, gateway_id, retryPosture);
            }
        }

        // ── 6. Threshold notifications ─────────────────────────────────────────
        if (status === 'FAILED' || status === 'CRITICAL') {
            checkAndNotify({ gateway_id }).catch((err) =>
                console.error('❌ PayPal notification error:', err.message),
            );
        }

        emitSocketEvent(io, gateway_id, event.event_type, status);
        return res.status(200).json({ received: true });

    } catch (err) {
        console.error('🔥 PayPal webhook controller error:', err.message);

        await logWebhook({
            gateway_id,
            provider_event_id:  event?.id ?? 'unknown',
            event_type:         event?.event_type ?? 'unknown',
            status:             'FAILED',
            http_status_code:   500,
            payload:            event ?? req.body,
            error_stack:        `System Error: ${err.message}`,
            last_error_message: 'internal_server_error',
            latency:            Date.now() - startTime,
        }).catch(() => {});

        return res.status(500).json({ error: 'Webhook processing failed.' });
    }
};

// ─── Analytics ────────────────────────────────────────────────────────────────

/**
 * GET /gateways/paypal/:gateway_id/analytics?filter=24h
 *
 * Returns summary stats, chart data, top errors, recent events, and
 * loss/recovery figures for a PayPal gateway in the given time window.
 *
 * PayPal amounts are decimal strings (e.g. "10.00") — no minor-unit conversion needed.
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
                 WHERE gateway_id    = $1
                   AND provider_type = 'PAYPAL'
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
                    AND w.provider_type = 'PAYPAL'
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
                   AND provider_type = 'PAYPAL'
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
            // PayPal amounts are decimal strings inside resource.amount.value
            db.query(
                `SELECT
                    id,
                    event_type,
                    status,
                    to_char(received_at, 'HH24:MI:SS')                                     AS time,
                    (payload->'resource'->'amount'->>'value')::numeric                      AS amount,
                    payload->'resource'->'amount'->>'currency_code'                         AS currency,
                    payload->'resource'->>'id'                                              AS capture_id,
                    payload->>'summary'                                                     AS summary,
                    payload->'resource'->'processor_response'->>'response_code'             AS response_code,
                    payload->'resource'->'processor_response'->>'response_code_description' AS decline_reason,
                    payload->'resource'->'supplementary_data'->'related_ids'->>'order_id'   AS order_id,
                    payload->'resource'->'payment_source'->'card'->>'last_digits'           AS card_last4,
                    payload->'resource'->'payment_source'->'card'->>'brand'                 AS card_brand
                 FROM webhook_logs
                 WHERE gateway_id    = $1
                   AND provider_type = 'PAYPAL'
                   AND status       != 'IGNORED'
                   AND event_type   IN (
                       'PAYMENT.CAPTURE.COMPLETED',
                       'PAYMENT.CAPTURE.DENIED',
                       'PAYMENT.CAPTURE.REFUNDED',
                       'PAYMENT.CAPTURE.REVERSED',
                       'CUSTOMER.DISPUTE.CREATED',
                       'CUSTOMER.DISPUTE.RESOLVED',
                       'BILLING.SUBSCRIPTION.CANCELLED',
                       'BILLING.SUBSCRIPTION.CREATED'
                   )
                 ORDER BY received_at DESC
                 LIMIT 10`,
                [gateway_id],
            ),

            // 5. Payment loss — PAYMENT.CAPTURE.DENIED only
            db.query(
                `SELECT
                    COALESCE(
                        SUM((payload->'resource'->'amount'->>'value')::numeric), 0
                    ) AS total_payment_loss,
                    COUNT(*) AS failed_charge_count
                 FROM webhook_logs
                 WHERE gateway_id    = $1
                   AND provider_type = 'PAYPAL'
                   AND event_type   = 'PAYMENT.CAPTURE.DENIED'
                   AND status       = 'FAILED'
                   AND received_at  >= NOW() - INTERVAL '${range}'
                   AND (payload->'resource'->'amount'->>'value') IS NOT NULL`,
                [gateway_id],
            ),

            // 6. Delivery failure loss — infrastructure errors
            db.query(
                `SELECT
                    COALESCE(
                        SUM((payload->'resource'->'amount'->>'value')::numeric), 0
                    ) AS total_delivery_loss,
                    COUNT(*) AS delivery_failure_count
                 FROM webhook_logs
                 WHERE gateway_id    = $1
                   AND provider_type = 'PAYPAL'
                   AND status       = 'FAILED'
                   AND event_type  != 'PAYMENT.CAPTURE.DENIED'
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
            provider:  'PAYPAL',
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
        console.error('❌ PayPal analytics error:', err.message);
        return res.status(500).json({ error: 'Analytics query failed.' });
    }
};

// ─── Webhook detail + RCA ─────────────────────────────────────────────────────

/**
 * GET /webhooks/paypal/:webhook_id
 *
 * Returns a single PayPal webhook log enriched with:
 *  - RCA rule match (rca_rules table via LATERAL join — shared with Stripe/Adyen)
 *  - Retry posture derived from processor_response.response_code
 *  - Failure classification (USER_FAILURE vs SYSTEM_FAILURE)
 *  - Plain-English RCA hint (rca_rules or local RCA_ADVISOR fallback)
 *
 * The rca_rules table uses error_code to match PayPal response codes
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

                -- Retry posture — meaningful for PAYMENT.CAPTURE.DENIED only
                CASE
                    WHEN w.event_type = 'PAYMENT.CAPTURE.DENIED' THEN
                        CASE
                            WHEN w.payload->'resource'->'processor_response'->>'response_code'
                                 IN ('0200','0500','0580','5160','5170','5650','9500','9520','9530','PPAD')
                            THEN 'DO_NOT_RETRY'
                            WHEN w.payload->'resource'->'processor_response'->>'response_code'
                                 IN ('0800','9600')
                            THEN 'RETRY'
                            ELSE 'RETRY_ONCE'
                        END
                END AS retry_posture,

                -- Failure classification
                CASE
                    WHEN w.event_type = 'PAYMENT.CAPTURE.DENIED'  THEN 'USER_FAILURE'
                    WHEN w.error_stack ILIKE ANY(ARRAY[
                             '%timeout%','%ECONNREFUSED%','%500%'
                         ])                                        THEN 'SYSTEM_FAILURE'
                    ELSE 'UNKNOWN'
                END AS failure_type

             FROM webhook_logs w
             LEFT JOIN LATERAL (
                 SELECT *
                 FROM rca_rules r
                 WHERE
                     -- Match PayPal processor response_code against error_code column
                     (r.error_code IS NOT NULL
                      AND w.payload->'resource'->'processor_response'->>'response_code'
                          = r.error_code)
                  OR -- Pattern fallback on last_error_message
                     (r.error_pattern IS NOT NULL
                      AND w.last_error_message ILIKE '%' || r.error_pattern || '%')
                 ORDER BY
                     CASE
                         WHEN r.error_code IS NOT NULL
                              AND w.payload->'resource'->'processor_response'->>'response_code'
                                  = r.error_code
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

        const row          = rows[0];
        const responseCode = row.payload?.resource?.processor_response?.response_code ?? null;
        const description  = row.payload?.resource?.processor_response?.response_code_description ?? null;

        const rcaHint = row.suggested_fix
                     ?? getRcaHint(responseCode, description);

        return res.status(200).json({ ...row, rca_hint: rcaHint });

    } catch (err) {
        console.error('❌ PayPal RCA engine error:', err.message);
        return res.status(500).json({ error: 'Failed to run RCA analysis.' });
    }
};

// ─── Replay ───────────────────────────────────────────────────────────────────

/**
 * POST /webhooks/paypal/:id/replay
 *
 * Re-delivers a stored PayPal webhook payload to the gateway's target URL.
 * Hard declines and dispute events are blocked.
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

        // Guard: block dispute events
        if (webhook.event_type === 'CUSTOMER.DISPUTE.CREATED'
         || webhook.event_type === 'CUSTOMER.DISPUTE.UPDATED') {
            return res.status(400).json({
                error:  'Replay blocked.',
                reason: 'Dispute events cannot be replayed — manage disputes in the PayPal Resolution Centre.',
            });
        }

        // Guard: block hard declines
        const responseCode = webhook.payload?.resource?.processor_response?.response_code ?? null;
        const retryPosture = getRetryPosture(responseCode);

        if (webhook.event_type === 'PAYMENT.CAPTURE.DENIED' && retryPosture === 'DO_NOT_RETRY') {
            return res.status(400).json({
                error:         'Replay blocked.',
                reason:        'Hard decline — replaying will yield the same result.',
                retry_posture: 'DO_NOT_RETRY',
                response_code: responseCode,
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
                webhook.payload,
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
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'PAYPAL')`,
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
        console.error('❌ PayPal replay error:', err.message);
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

const scheduleAutoRetryIfConfigured = async (event, gatewayId, retryPosture) => {
    try {
        const [gatewayRes, logRes] = await Promise.all([
            db.query('SELECT webhook_url FROM gateways WHERE id = $1', [gatewayId]),
            db.query(
                `SELECT id FROM webhook_logs
                 WHERE provider_event_id = $1 AND gateway_id = $2
                 ORDER BY received_at DESC LIMIT 1`,
                [event.id, gatewayId],
            ),
        ]);

        const targetUrl    = gatewayRes.rows[0]?.webhook_url;
        const webhookLogId = logRes.rows[0]?.id;

        if (!targetUrl || !webhookLogId) return;

        await scheduleRetry({
            webhookLogId,
            gatewayId,
            payload:       event,
            targetUrl,
            attemptNumber: 1,
            retryPosture,
        });
    } catch (err) {
        console.error('⚠️  PayPal auto-retry scheduling failed (non-fatal):', err.message);
    }
};

const formatUsd = (value) => `$${value.toFixed(2)}`;

// ─── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
    handlePaypalWebhook,
    getGatewayAnalytics,
    getWebhookDetails,
    replayWebhook,
};