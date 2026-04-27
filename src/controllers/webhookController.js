const { logWebhook } = require('../models/webhookModels');
const db = require('../config/db');
const axios = require("axios");
const { scheduleRetry } = require('../queues/retryQueue');
const { checkAndNotify } = require('../services/notificationService');

// ─────────────────────────────────────────────
// RCA ADVISOR — maps decline/error codes to
// plain-English advice for non-tech users
// ─────────────────────────────────────────────
const rcaAdvisor = {
    // Decline codes (from charge.failed → outcome.reason or last_payment_error.decline_code)
    'generic_decline':       'Bank-side security block. Suggest customer contact their bank or try an international-enabled card.',
    'insufficient_funds':    'Customer balance is too low. Suggest a top-up or a different card.',
    'expired_card':          'Card is expired. Ask customer to update their payment method.',
    'fraudulent':            'Transaction flagged as high risk. Do NOT retry — risk of chargeback.',
    'incorrect_cvv':         'CVV mismatch. Ask customer to re-enter card details carefully.',
    'do_not_honor':          'Bank rejected without reason. Customer should contact their bank directly.',
    'card_not_supported':    'Card type not supported by this gateway. Try a different card.',
    'card_declined':         'Card was declined by the issuing bank.',
    'incorrect_number':      'Invalid card number entered. Ask customer to double-check.',
    'processing_error':      'Temporary gateway issue. Safe to retry once after a short delay.',
    'rate_limit':            'Too many requests sent to the gateway. Retry after a brief delay.',
    'do_not_retry':          'Hard decline. Do not retry — this will not succeed.',
    'try_again_later':       'Temporary issue. Safe to retry automatically.',
};

// ─────────────────────────────────────────────
// HELPER: should this charge.failed be retried?
// Uses Stripe's advice_code from outcome object
// ─────────────────────────────────────────────
const getRetryPosture = (outcomeAdviceCode, declineCode) => {
    if (declineCode === 'fraudulent' || declineCode === 'do_not_honor') return 'DO_NOT_RETRY';
    if (outcomeAdviceCode === 'do_not_retry') return 'DO_NOT_RETRY';
    if (outcomeAdviceCode === 'try_again_later') return 'RETRY';
    return 'RETRY_ONCE'; // cautious default
};


// ─────────────────────────────────────────────
// HANDLER: Stripe Webhook Ingestion
// Key fix: only charge.failed is treated as the
// canonical payment failure. payment_intent.payment_failed
// is logged as IGNORED to prevent double-counting.
// ─────────────────────────────────────────────
const handleStripeWebhook = async (req, res) => {
    const io = req.app.get('socketio');
    const { gateway_id } = req.params;
    const event = req.stripeEvent;
    const startTime = Date.now();

    let status = 'SUCCESS';
    let errorMessage = null;
    let errorStack = null;

    try {
        // ── payment_intent.payment_failed ──────────────────────────────
        // IGNORE for counting — charge.failed is the source of truth.
        // We still log it so the audit trail is complete, but mark IGNORED
        // so it never appears in failure counts or loss calculations.
        if (event.type === 'payment_intent.payment_failed') {
            await logWebhook({
                gateway_id,
                provider_event_id: event.id,
                event_type: event.type,
                status: 'IGNORED',          // never counted as a failure
                http_status: 200,
                payload: event,
                error_stack: null,
                last_error_message: null,
                latency: Date.now() - startTime,
            });

            // Still emit so the client knows an event arrived
            if (io) {
                io.emit(`update_dashboard_${gateway_id}`, {
                    event: event.type,
                    status: 'IGNORED',
                    timestamp: new Date(),
                });
            }

            return res.status(200).send({ received: true, note: 'Duplicate of charge.failed — ignored for counting' });
        }

        // ── charge.failed ──────────────────────────────────────────────
        // This is the canonical payment failure event.
        // Extract everything from outcome (richer than last_payment_error).
        if (event.type === 'charge.failed') {
            status = 'FAILED';

            const obj = event.data.object;
            const outcome = obj.outcome || {};
            const declineCode = outcome.reason || obj.failure_code || null;
            const adviceCode  = outcome.advice_code || null;
            const rawMessage  = obj.failure_message || outcome.seller_message || 'Transaction declined';

            // errorMessage drives RCA rule matching (stored in last_error_message)
            errorMessage = declineCode || obj.failure_code || rawMessage;

            errorStack = JSON.stringify({
                failure_code:       obj.failure_code,
                decline_code:       declineCode,
                advice_code:        adviceCode,
                retry_posture:      getRetryPosture(adviceCode, declineCode),
                network_status:     outcome.network_status,
                risk_score:         outcome.risk_score,
                seller_message:     outcome.seller_message,
                raw_message:        rawMessage,
                rca_hint:           rcaAdvisor[declineCode] || rcaAdvisor[obj.failure_code] || 'Unknown decline — check bank.',
            });
        }

        // ── dispute events ─────────────────────────────────────────────
        else if (event.type.includes('dispute')) {
            status = 'CRITICAL';
            errorMessage = 'dispute_initiated';
            errorStack = JSON.stringify({
                rca_hint: 'A chargeback dispute has been raised. Respond with evidence within the deadline.',
            });
        }

        // ── All other events (succeeded, refunded, subscription, etc.) ─
        // status stays SUCCESS, errorMessage/errorStack stay null

        const latency = Date.now() - startTime;

        await logWebhook({
            gateway_id,
            provider_event_id: event.id,
            event_type: event.type,
            status,
            http_status: 200,
            payload: event,
            error_stack: errorStack,
            last_error_message: errorMessage,
            latency,
        });

        // ── Auto Retry Scheduling ──────────────────────────────────
        // Only for charge.failed with RETRY posture AND target URL set
        if (event.type === 'charge.failed' && status === 'FAILED') {
            const retryPosture = getRetryPosture(
                event.data.object?.outcome?.advice_code,
                event.data.object?.outcome?.reason
            );

            if (retryPosture === 'RETRY' || retryPosture === 'RETRY_ONCE') {
                // Get target URL from gateway
                const gatewayRes = await db.query(
                    `SELECT webhook_url FROM gateways WHERE id = $1`,
                    [gateway_id]
                );

                const targetUrl = gatewayRes.rows[0]?.webhook_url;

                if (targetUrl) {
                    const logRes = await db.query(
                        `SELECT id FROM webhook_logs 
                         WHERE provider_event_id = $1 AND gateway_id = $2`,
                        [event.id, gateway_id]
                    );

                    const webhookLogId = logRes.rows[0]?.id;

                    if (webhookLogId) {
                        await scheduleRetry({
                            webhookLogId,
                            gatewayId: gateway_id,
                            payload: event,
                            targetUrl,
                            attemptNumber: 1,
                        });
                    }
                }
            }
        }

        // Check thresholds and notify if breached
        if (status === 'FAILED' || status === 'CRITICAL') {
            checkAndNotify({ gateway_id }).catch(err =>
                console.error('❌ Notification error:', err.message)
            );
        }

        if (io) {
            io.emit(`update_dashboard_${gateway_id}`, {
                event: event.type,
                status,
                timestamp: new Date(),
            });
        }

        res.status(200).send({ received: true });

    } catch (err) {
        console.error('🔥 Webhook Controller Error:', err.message);

        await logWebhook({
            gateway_id,
            provider_event_id: event?.id || 'unknown',
            event_type: event?.type || 'unknown',
            status: 'FAILED',
            http_status: 500,
            payload: event || req.body,
            error_stack: `System Error: ${err.message}`,
            last_error_message: 'internal_server_error',
            latency: Date.now() - startTime,
        });

        res.status(500).json({ error: 'Webhook processing failed' });
    }
};


// ─────────────────────────────────────────────
// ANALYTICS: Main dashboard data endpoint
// ─────────────────────────────────────────────
const getGatewayAnalytics = async (req, res) => {
    const { gateway_id } = req.params;
    const { filter = '24h' } = req.query;

    const intervals = {
        '1h':  { gap: '1 minute', range: '1 hour',  trunc: 'minute', format: 'HH24:MI' },
        '24h': { gap: '1 hour',   range: '24 hours', trunc: 'hour',  format: 'HH24:00' },
        '7d':  { gap: '1 day',    range: '7 days',   trunc: 'day',   format: 'DD Mon'  },
        '30d': { gap: '1 day',    range: '30 days',  trunc: 'day',   format: 'DD Mon'  },
        '1y':  { gap: '1 month',  range: '1 year',   trunc: 'month', format: 'Mon YYYY'},
    };

    const config = intervals[filter] || intervals['24h'];

    try {
        // ── 1. Summary Stats ───────────────────────────────────────────
        // Excludes IGNORED rows so payment_intent.payment_failed
        // never inflates total or failure counts.
        const summaryQuery = `
            SELECT 
                COUNT(*) FILTER (WHERE status != 'IGNORED')                    AS total_count,
                COUNT(*) FILTER (WHERE status = 'SUCCESS')                     AS success_count,
                COUNT(*) FILTER (WHERE status IN ('FAILED', 'CRITICAL'))       AS failure_count
            FROM webhook_logs
            WHERE gateway_id = $1
            AND received_at >= NOW() - INTERVAL '${config.range}';
        `;

        // ── 2. Chart Data ──────────────────────────────────────────────
        const chartQuery = `
            SELECT 
                to_char(series.time_slot, '${config.format}') AS label,
                COALESCE(COUNT(w.id) FILTER (WHERE w.status = 'SUCCESS'), 0)                  AS success,
                COALESCE(COUNT(w.id) FILTER (WHERE w.status IN ('FAILED', 'CRITICAL')), 0)    AS failed
            FROM generate_series(
                date_trunc('${config.trunc}', NOW() - INTERVAL '${config.range}') + INTERVAL '${config.gap}',
                date_trunc('${config.trunc}', NOW()),
                '${config.gap}'::interval
            ) AS series(time_slot)
            LEFT JOIN webhook_logs w 
                ON date_trunc('${config.trunc}', w.received_at) = series.time_slot
                AND w.gateway_id = $1
                AND w.status != 'IGNORED'
            GROUP BY series.time_slot
            ORDER BY series.time_slot ASC;
        `;

        // ── 3. Top Errors (RCA table) ──────────────────────────────────
        const errorsQuery = `
            SELECT 
                last_error_message,
                COUNT(*) AS occurrence
            FROM webhook_logs
            WHERE gateway_id = $1
            AND status IN ('FAILED', 'CRITICAL')
            AND received_at >= NOW() - INTERVAL '${config.range}'
            AND last_error_message IS NOT NULL
            AND last_error_message != ''
            GROUP BY last_error_message
            ORDER BY occurrence DESC
            LIMIT 5;
        `;

        // ── 4. Recent Hits (Activity Feed) ────────────────────────────
        // charge.failed is the single source of truth for payment failures.
        // payment_intent.payment_failed is excluded (status = IGNORED handles it,
        // but we also exclude by event_type for clarity).
        const recentHitsQuery = `
            SELECT 
                id,
                event_type,
                status,
                to_char(received_at, 'HH24:MI:SS')                                             AS time,

                -- Amount in dollars
                CASE
                    WHEN (payload #>> '{data,object,amount}') IS NOT NULL
                    THEN (payload #>> '{data,object,amount}')::numeric / 100
                    ELSE NULL
                END                                                                             AS amount,

                payload #>> '{data,object,currency}'                                            AS currency,

                -- Retry posture (only populated on charge.failed)
                payload #>> '{data,object,outcome,advice_code}'                                 AS retry_advice,

                -- Plain-English decline reason from the bank
                payload #>> '{data,object,outcome,seller_message}'                              AS decline_reason,

                -- Failure code for RCA matching
                payload #>> '{data,object,failure_code}'                                        AS failure_code,

                -- Risk score for fraud flagging
                (payload #>> '{data,object,outcome,risk_score}')::numeric                      AS risk_score,

                -- Card details for reconciliation
                payload #>> '{data,object,payment_method_details,card,last4}'                  AS card_last4,
                payload #>> '{data,object,payment_method_details,card,brand}'                  AS card_brand,

                -- Link to payment intent for grouping
                payload #>> '{data,object,payment_intent}'                                      AS payment_intent_id

            FROM webhook_logs
            WHERE gateway_id = $1
            AND status != 'IGNORED'
            AND event_type IN (
                'payment_intent.succeeded',
                'charge.failed',
                'charge.refunded',
                'customer.subscription.deleted',
                'customer.subscription.created'
            )
            ORDER BY received_at DESC
            LIMIT 10;
        `;

        // ── 5. Payment Loss (Revenue Impact) ──────────────────────────
        // Uses ONLY charge.failed — single source of truth, no double-counting.
        const paymentLossQuery = `
            SELECT
                COALESCE(SUM(
                    (payload #>> '{data,object,amount}')::numeric / 100
                ), 0)           AS total_payment_loss,
                COUNT(*)        AS failed_charge_count
            FROM webhook_logs
            WHERE gateway_id = $1
            AND event_type = 'charge.failed'
            AND status = 'FAILED'
            AND received_at >= NOW() - INTERVAL '${config.range}'
            AND (payload #>> '{data,object,amount}') IS NOT NULL;
        `;

        // ── 6. Delivery Failure Loss (Recoverable) ────────────────────
        // These are events YOUR system failed to process (timeouts, 500s).
        // These ARE safe to retry regardless of advice_code.
        // Excludes charge.failed because those are bank declines, not delivery issues.
        const deliveryFailureLossQuery = `
            SELECT
                COALESCE(SUM(
                    (payload #>> '{data,object,amount}')::numeric / 100
                ), 0)           AS total_delivery_loss,
                COUNT(*)        AS delivery_failure_count
            FROM webhook_logs
            WHERE gateway_id = $1
            AND status = 'FAILED'
            AND event_type != 'charge.failed'
            AND received_at >= NOW() - INTERVAL '${config.range}'
            AND (
                error_stack ILIKE '%timeout%'
                OR error_stack ILIKE '%connection%'
                OR error_stack ILIKE '%ECONNREFUSED%'
                OR error_stack ILIKE '%500%'
                OR http_status = 500
            );
        `;

        // ── 7. Loss Recovered ─────────────────────────────────────────
        const recoveryQuery = `
            SELECT
                COALESCE(SUM(amount), 0)    AS total_recovered,
                COUNT(*)                    AS recovery_count,
                COUNT(*) FILTER (WHERE recovery_type = 'AUTO_RETRY')   AS auto_retry_count,
                COUNT(*) FILTER (WHERE recovery_type = 'MANUAL_RETRY') AS manual_retry_count
            FROM recovery_logs
            WHERE gateway_id = $1
            AND recovered_at >= NOW() - INTERVAL '${config.range}';
        `;

        const [summaryRes, chartRes, errorsRes, recentHitsRes, paymentLossRes, deliveryLossRes, recoveryRes] = await Promise.all([
            db.query(summaryQuery,         [gateway_id]),
            db.query(chartQuery,           [gateway_id]),
            db.query(errorsQuery,          [gateway_id]),
            db.query(recentHitsQuery,      [gateway_id]),
            db.query(paymentLossQuery,     [gateway_id]),
            db.query(deliveryFailureLossQuery, [gateway_id]),
            db.query(recoveryQuery,            [gateway_id]),
        ]);

        const stats   = summaryRes.rows[0];
        const total   = parseInt(stats.total_count);
        const success = parseInt(stats.success_count);
        const failure = parseInt(stats.failure_count);

        const successRate     = total > 0 ? Math.round((success / total) * 100) : 0;
        const paymentLoss     = parseFloat(paymentLossRes.rows[0].total_payment_loss  || 0);
        const deliveryLoss    = parseFloat(deliveryLossRes.rows[0].total_delivery_loss || 0);
        const totalLoss       = paymentLoss + deliveryLoss;
        const totalRecovered  = parseFloat(recoveryRes.rows[0].total_recovered || 0);
        const netLoss         = Math.max(0, totalLoss - totalRecovered);

        res.status(200).json({
            gateway_id,
            filter,
            summary: {
                total_events:   total,
                success_events: success,
                failed_events:  failure,
                success_rate:   `${successRate}%`,
            },
            loss: {
                payment_loss: {
                    amount:       `$${paymentLoss.toFixed(2)}`,
                    raw:          paymentLoss,
                    charge_count: parseInt(paymentLossRes.rows[0].failed_charge_count || 0),
                },
                delivery_loss: {
                    amount:        `$${deliveryLoss.toFixed(2)}`,
                    raw:           deliveryLoss,
                    failure_count: parseInt(deliveryLossRes.rows[0].delivery_failure_count || 0),
                },
                total_at_risk: {
                    amount: `$${totalLoss.toFixed(2)}`,
                    raw:    totalLoss,
                },
                // Loss Recovered — from successful auto/manual retries
                recovered: {
                    amount:              `$${totalRecovered.toFixed(2)}`,
                    raw:                 totalRecovered,
                    recovery_count:      parseInt(recoveryRes.rows[0].recovery_count || 0),
                    auto_retry_count:    parseInt(recoveryRes.rows[0].auto_retry_count || 0),
                    manual_retry_count:  parseInt(recoveryRes.rows[0].manual_retry_count || 0),
                },
                // Net loss after recovery — the most important number
                net_loss: {
                    amount: `$${netLoss.toFixed(2)}`,
                    raw:    netLoss,
                },
            },
            top_errors:   errorsRes.rows,
            chart_data:   chartRes.rows,
            recent_hits:  recentHitsRes.rows,
        });

    } catch (err) {
        console.error('❌ Analytics API Error:', err.message);
        res.status(500).json({ error: `Analytics failed: ${err.message}` });
    }
};


// ─────────────────────────────────────────────
// WEBHOOK DETAIL: Single event with RCA enrichment
// ─────────────────────────────────────────────
const getWebhookDetails = async (req, res) => {
    const { webhook_id } = req.params;

    try {
        const query = `
            SELECT
                w.*,
                r.issue_category,
                r.suggested_fix,
                r.severity AS rca_severity,

                -- Retry posture derived from payload (charge.failed only)
                CASE
                    WHEN w.event_type = 'charge.failed' THEN
                        CASE
                            WHEN w.payload #>> '{data,object,outcome,advice_code}' = 'do_not_retry'
                                 OR w.payload #>> '{data,object,outcome,reason}' IN ('fraudulent', 'do_not_honor')
                            THEN 'DO_NOT_RETRY'
                            WHEN w.payload #>> '{data,object,outcome,advice_code}' = 'try_again_later'
                            THEN 'RETRY'
                            ELSE 'RETRY_ONCE'
                        END
                    ELSE NULL
                END AS retry_posture,

                -- Failure classification: USER (bank decline) vs SYSTEM (infrastructure)
                CASE
                    WHEN w.event_type = 'charge.failed' THEN 'USER_FAILURE'
                    WHEN w.error_stack ILIKE '%timeout%'
                      OR w.error_stack ILIKE '%ECONNREFUSED%'
                      OR w.error_stack ILIKE '%500%'
                    THEN 'SYSTEM_FAILURE'
                    ELSE 'UNKNOWN'
                END AS failure_type

            FROM webhook_logs w
            LEFT JOIN LATERAL (
                SELECT *
                FROM rca_rules r
                WHERE
                    (
                        r.decline_code IS NOT NULL
                        AND COALESCE(
                            w.payload #>> '{data,object,last_payment_error,decline_code}',
                            w.payload #>> '{data,object,outcome,reason}'
                        ) = r.decline_code
                    )
                    OR
                    (
                        r.error_code IS NOT NULL
                        AND COALESCE(
                            w.payload #>> '{data,object,last_payment_error,code}',
                            w.payload #>> '{data,object,failure_code}'
                        ) = r.error_code
                    )
                    OR
                    (
                        r.error_pattern IS NOT NULL
                        AND w.last_error_message ILIKE '%' || r.error_pattern || '%'
                    )
                ORDER BY
                    CASE
                        WHEN r.decline_code IS NOT NULL
                             AND COALESCE(
                                 w.payload #>> '{data,object,last_payment_error,decline_code}',
                                 w.payload #>> '{data,object,outcome,reason}'
                             ) = r.decline_code THEN 3
                        WHEN r.error_code IS NOT NULL
                             AND COALESCE(
                                 w.payload #>> '{data,object,last_payment_error,code}',
                                 w.payload #>> '{data,object,failure_code}'
                             ) = r.error_code THEN 2
                        WHEN r.error_pattern IS NOT NULL
                             AND w.last_error_message ILIKE '%' || r.error_pattern || '%' THEN 1
                        ELSE 0
                    END DESC,
                    r.id DESC
                LIMIT 1
            ) r ON true
            WHERE w.id = $1;
        `;

        const result = await db.query(query, [webhook_id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Webhook not found' });
        }

        const row = result.rows[0];

        // Attach a plain-English RCA hint from our local advisor map
        // as a fallback if the rca_rules table has no match
        const declineCode = row.payload?.data?.object?.outcome?.reason
            || row.payload?.data?.object?.failure_code;

        const rcaHint = row.suggested_fix
            || rcaAdvisor[declineCode]
            || rcaAdvisor[row.last_error_message]
            || 'Unknown failure — review payload for details.';

        res.status(200).json({
            ...row,
            rca_hint: rcaHint,
        });

    } catch (err) {
        console.error('❌ RCA Engine Error:', err.message);
        res.status(500).json({ error: 'Failed to run RCA analysis' });
    }
};


// ─────────────────────────────────────────────
// REPLAY: Re-send a webhook to its target URL
// ─────────────────────────────────────────────
const replayWebhook = async (req, res) => {
    const { id } = req.params;

    try {
        const result = await db.query(
            `SELECT wl.*, g.webhook_secret, g.target_url AS webhook_url
             FROM webhook_logs wl
             JOIN gateways g ON wl.gateway_id = g.id
             WHERE wl.id = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Webhook not found' });
        }

        const webhook = result.rows[0];

        // Safety guard: don't replay USER_FAILURE charge.failed events
        // marked DO_NOT_RETRY — replaying a bank decline is pointless
        const outcomeAdviceCode = webhook.payload?.data?.object?.outcome?.advice_code;
        const declineCode       = webhook.payload?.data?.object?.outcome?.reason;
        const retryPosture      = getRetryPosture(outcomeAdviceCode, declineCode);

        if (webhook.event_type === 'charge.failed' && retryPosture === 'DO_NOT_RETRY') {
            return res.status(400).json({
                error:         'Replay blocked',
                reason:        'This is a hard bank decline — replaying will yield the same result.',
                retry_posture: 'DO_NOT_RETRY',
                decline_code:  declineCode,
            });
        }

        const targetUrl = webhook.webhook_url;
        if (!targetUrl) {
            return res.status(400).json({ error: 'No target URL configured for this gateway' });
        }

        const start = Date.now();
        let response    = null;
        let status      = 'SUCCESS';
        let errorStack  = null;

        try {
            response = await axios.post(targetUrl, webhook.payload, {
                headers: webhook.request_headers || {},
                timeout: 5000,
            });
        } catch (err) {
            status     = 'FAILED';
            errorStack = err.message;
        }

        const latency = Date.now() - start;

        await db.query(
            `INSERT INTO webhook_logs (
                gateway_id,
                provider_event_id,
                event_type,
                status,
                http_status,
                latency_ms,
                payload,
                request_headers,
                error_stack,
                last_error_message,
                retry_count,
                replayed_from
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
            [
                webhook.gateway_id,
                `${webhook.provider_event_id}_replay`,
                webhook.event_type,
                status,
                response?.status || 500,
                latency,
                webhook.payload,
                webhook.request_headers,
                errorStack,
                errorStack,
                (webhook.retry_count || 0) + 1,
                webhook.id,
            ]
        );

        return res.json({
            message:       'Webhook replayed',
            status,
            latency_ms:    latency,
            retry_posture: retryPosture,
        });

    } catch (err) {
        console.error('Replay error:', err);
        res.status(500).json({ error: 'Replay failed' });
    }
};


module.exports = {
    handleStripeWebhook,
    getGatewayAnalytics,
    getWebhookDetails,
    replayWebhook,
};