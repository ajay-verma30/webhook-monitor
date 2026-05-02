"use strict";

/**
 * webhookController.js
 *
 * Handles Stripe webhook ingestion, analytics, RCA enrichment, and event replay.
 *
 * Design principles:
 *  - charge.failed is the single source of truth for payment failures.
 *  - payment_intent.payment_failed is logged as IGNORED to prevent double-counting.
 *  - All DB calls are parameterised; no raw string interpolation for user input.
 *  - Retry posture is derived from Stripe's advice_code + decline_code, never guessed.
 *  - Every public function validates its inputs before touching the DB.
 *  - original_status is set on INSERT only — never overwritten on upsert — so
 *    recovered payments always appear in the failed count.
 */

const { logWebhook } = require("../models/webhookModels");
const db = require("../config/db");
const axios = require("axios");
const { scheduleRetry } = require("../queues/retryQueue");
const { checkAndNotify } = require("../services/notificationService");

// ─── Constants ────────────────────────────────────────────────────────────────

const RCA_ADVISOR = Object.freeze({
  generic_decline:
    "Bank-side security block. Suggest the customer contact their bank or try an internationally-enabled card.",
  insufficient_funds:
    "Customer balance is too low. Suggest a top-up or a different card.",
  expired_card:
    "Card is expired. Ask the customer to update their payment method.",
  fraudulent:
    "Transaction flagged as high risk. Do NOT retry — chargeback risk.",
  incorrect_cvv:
    "CVV mismatch. Ask the customer to re-enter card details carefully.",
  do_not_honor:
    "Bank rejected without reason. The customer should contact their bank directly.",
  card_not_supported:
    "Card type not supported by this gateway. Try a different card.",
  card_declined: "Card was declined by the issuing bank.",
  incorrect_number:
    "Invalid card number entered. Ask the customer to double-check.",
  processing_error:
    "Temporary gateway issue. Safe to retry once after a short delay.",
  rate_limit:
    "Too many requests sent to the gateway. Retry after a brief delay.",
  do_not_retry: "Hard decline. Do not retry — this will not succeed.",
  try_again_later: "Temporary issue. Safe to retry automatically.",
});

const HARD_DECLINE_CODES = new Set([
  "fraudulent",
  "do_not_honor",
  "do_not_retry",
]);

const ANALYTICS_INTERVALS = Object.freeze({
  "1h":  { gap: "1 minute", range: "1 hour",   trunc: "minute", format: "HH24:MI"   },
  "24h": { gap: "1 hour",   range: "24 hours",  trunc: "hour",   format: "HH24:00"   },
  "7d":  { gap: "1 day",    range: "7 days",    trunc: "day",    format: "DD Mon"     },
  "30d": { gap: "1 day",    range: "30 days",   trunc: "day",    format: "DD Mon"     },
  "1y":  { gap: "1 month",  range: "1 year",    trunc: "month",  format: "Mon YYYY"   },
});

const DEFAULT_FILTER    = "24h";
const REPLAY_TIMEOUT_MS = 5_000;

const CANONICAL_EVENTS = `(
    'charge.succeeded',
    'charge.failed',
    'charge.refunded',
    'payment_intent.succeeded',
    'payment_intent.payment_failed',
    'customer.subscription.deleted',
    'customer.subscription.created'
)`;

// ─── Pure helpers ─────────────────────────────────────────────────────────────

const getRetryPosture = (adviceCode, declineCode) => {
  if (HARD_DECLINE_CODES.has(declineCode) || adviceCode === "do_not_retry") {
    return "DO_NOT_RETRY";
  }
  if (adviceCode === "try_again_later") return "RETRY";
  return "RETRY_ONCE";
};

const getRcaHint = (primaryCode, fallbackCode) =>
  RCA_ADVISOR[primaryCode] ??
  RCA_ADVISOR[fallbackCode] ??
  "Unknown decline — check the payload for details.";

const extractChargeFailedMeta = (obj) => {
  const outcome     = obj.outcome ?? {};
  const declineCode = outcome.reason ?? obj.failure_code ?? null;
  const adviceCode  = outcome.advice_code ?? null;
  const rawMessage  =
    obj.failure_message ?? outcome.seller_message ?? "Transaction declined";

  const retryPosture = getRetryPosture(adviceCode, declineCode);

  const errorMessage = declineCode ?? obj.failure_code ?? rawMessage;
  const errorStack   = JSON.stringify({
    failure_code:   obj.failure_code ?? null,
    decline_code:   declineCode,
    advice_code:    adviceCode,
    retry_posture:  retryPosture,
    network_status: outcome.network_status ?? null,
    risk_score:     outcome.risk_score ?? null,
    seller_message: outcome.seller_message ?? null,
    raw_message:    rawMessage,
    rca_hint:       getRcaHint(declineCode, obj.failure_code),
  });

  return { declineCode, adviceCode, errorMessage, errorStack, retryPosture };
};

// ─── Webhook ingestion ────────────────────────────────────────────────────────

const handleStripeWebhook = async (req, res) => {
  const io          = req.app.get("socketio");
  const { gateway_id } = req.params;
  const event       = req.stripeEvent;
  const startTime   = Date.now();

  let status       = "SUCCESS";
  let errorMessage = null;
  let errorStack   = null;

  try {
    // ── 1. payment_intent.payment_failed — log as IGNORED ─────────
    if (event.type === "payment_intent.payment_failed") {
      await logWebhook({
        gateway_id,
        provider_event_id: event.id,
        event_type:        event.type,
        status:            "IGNORED",
        http_status:       200,
        payload:           event,
        error_stack:       null,
        last_error_message: null,
        latency:           Date.now() - startTime,
      });

      emitSocketEvent(io, gateway_id, event.type, "IGNORED");
      return res.status(200).json({
        received: true,
        note: "Duplicate of charge.failed — ignored for counting.",
      });
    }

    // ── 2. charge.failed ──────────────────────────────────────────
    if (event.type === "charge.failed") {
      status = "FAILED";
      const meta = extractChargeFailedMeta(event.data.object);
      errorMessage = meta.errorMessage;
      errorStack   = meta.errorStack;
    }

    // ── 3. Dispute events ─────────────────────────────────────────
    else if (event.type.startsWith("charge.dispute")) {
      status       = "CRITICAL";
      errorMessage = "dispute_initiated";
      errorStack   = JSON.stringify({
        rca_hint:
          "A chargeback dispute has been raised. Respond with evidence before the deadline.",
      });
    }

    // ── 4. All other events — status stays SUCCESS ─────────────────

    await logWebhook({
      gateway_id,
      provider_event_id:  event.id,
      event_type:         event.type,
      status,
      http_status:        200,
      payload:            event,
      error_stack:        errorStack,
      last_error_message: errorMessage,
      latency:            Date.now() - startTime,
    });

    // ── 5. Schedule auto-retry for retryable charge failures ──────
    if (event.type === "charge.failed" && status === "FAILED") {
      const obj          = event.data.object;
      const retryPosture = getRetryPosture(
        obj?.outcome?.advice_code,
        obj?.outcome?.reason,
      );
      if (retryPosture !== "DO_NOT_RETRY") {
        await scheduleAutoRetryIfConfigured(event, gateway_id, retryPosture);
      }
    }

    // ── 6. Threshold notifications ────────────────────────────────
    if (status === "FAILED" || status === "CRITICAL") {
      checkAndNotify({ gateway_id }).catch((err) =>
        console.error("❌ Notification error:", err.message),
      );
    }

    emitSocketEvent(io, gateway_id, event.type, status);
    return res.status(200).json({ received: true });

  } catch (err) {
    console.error("🔥 Webhook controller error:", err.message);

    await logWebhook({
      gateway_id,
      provider_event_id:  event?.id ?? "unknown",
      event_type:         event?.type ?? "unknown",
      status:             "FAILED",
      http_status:        500,
      payload:            event ?? req.body,
      error_stack:        `System Error: ${err.message}`,
      last_error_message: "internal_server_error",
      latency:            Date.now() - startTime,
    }).catch(() => {});

    return res.status(500).json({ error: "Webhook processing failed." });
  }
};

// ─── Analytics ────────────────────────────────────────────────────────────────

const getGatewayAnalytics = async (req, res) => {
  const { gateway_id } = req.params;
  const filterKey = ANALYTICS_INTERVALS[req.query.filter]
    ? req.query.filter
    : DEFAULT_FILTER;
  const cfg = ANALYTICS_INTERVALS[filterKey];
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
        `SELECT is_active
         FROM gateways
         WHERE id = $1`,
        [gateway_id],
      ),

      // 1. Summary
      // total_count and success_count use current status.
      // failure_count uses original_status so recovered payments still count as failures.
      db.query(
        `SELECT
  COUNT(*) FILTER (WHERE status != 'IGNORED')                      AS total_count,
  COUNT(*) FILTER (WHERE status = 'SUCCESS' 
                   AND original_status = 'SUCCESS')                 AS success_count,  -- pure successes only
  COUNT(*) FILTER (WHERE original_status IN ('FAILED','CRITICAL'))  AS failure_count
         FROM webhook_logs
         WHERE gateway_id  = $1
           AND received_at >= NOW() - INTERVAL '${range}'
           AND event_type  IN ${CANONICAL_EVENTS}`,
        [gateway_id],
      ),

      // 2. Time-series chart
      // success  → current status (what is it now)
      // failed   → original_status (did it ever fail, regardless of recovery)
      // recovered → recovery_logs (how many were saved)
      db.query(
        `SELECT
            to_char(series.time_slot, '${format}')                                              AS label,
            COALESCE(COUNT(w.id) FILTER (WHERE w.status = 'SUCCESS'), 0)                        AS success,
            COALESCE(COUNT(w.id) FILTER (WHERE w.original_status IN ('FAILED','CRITICAL')), 0)  AS failed,
            COALESCE(COUNT(r.id), 0)                                                             AS recovered
         FROM generate_series(
             date_trunc('${trunc}', NOW() - INTERVAL '${range}') + INTERVAL '${gap}',
             date_trunc('${trunc}', NOW()),
             '${gap}'::interval
         ) AS series(time_slot)
         LEFT JOIN webhook_logs w
             ON date_trunc('${trunc}', w.received_at) = series.time_slot
            AND w.gateway_id  = $1
            AND w.status     != 'IGNORED'
            AND w.event_type  IN ${CANONICAL_EVENTS}
         LEFT JOIN recovery_logs r
             ON date_trunc('${trunc}', r.recovered_at) = series.time_slot
            AND r.gateway_id = $1
         GROUP BY series.time_slot
         ORDER BY series.time_slot ASC`,
        [gateway_id],
      ),

      // 3. Top 5 error codes — use original_status so recovered failures still appear
      db.query(
        `SELECT
            last_error_message,
            COUNT(*) AS occurrence
         FROM webhook_logs
         WHERE gateway_id          = $1
           AND original_status     IN ('FAILED','CRITICAL')
           AND received_at         >= NOW() - INTERVAL '${range}'
           AND event_type          IN ${CANONICAL_EVENTS}
           AND last_error_message   IS NOT NULL
           AND last_error_message   != ''
         GROUP BY last_error_message
         ORDER BY occurrence DESC
         LIMIT 5`,
        [gateway_id],
      ),

      // 4. Recent activity feed — status reflects current state (shows recovery)
      db.query(
        `SELECT
            id,
            event_type,
            status,
            original_status,
            to_char(received_at, 'HH24:MI:SS')                                              AS time,
            CASE
                WHEN (payload #>> '{data,object,amount}') IS NOT NULL
                THEN (payload #>> '{data,object,amount}')::numeric / 100
            END                                                                              AS amount,
            payload #>> '{data,object,currency}'                                             AS currency,
            payload #>> '{data,object,outcome,advice_code}'                                  AS retry_advice,
            payload #>> '{data,object,outcome,seller_message}'                               AS decline_reason,
            payload #>> '{data,object,failure_code}'                                         AS failure_code,
            (payload #>> '{data,object,outcome,risk_score}')::numeric                        AS risk_score,
            payload #>> '{data,object,payment_method_details,card,last4}'                    AS card_last4,
            payload #>> '{data,object,payment_method_details,card,brand}'                    AS card_brand,
            payload #>> '{data,object,payment_intent}'                                       AS payment_intent_id
         FROM webhook_logs
         WHERE gateway_id = $1
           AND status    != 'IGNORED'
           AND event_type IN (
               'payment_intent.succeeded',
               'charge.failed',
               'charge.refunded',
               'customer.subscription.deleted',
               'customer.subscription.created'
           )
         ORDER BY received_at DESC
         LIMIT 10`,
        [gateway_id],
      ),

      // 5. Payment loss — use original_status so rows recovered via retry still count
      db.query(
        `SELECT
            COALESCE(SUM((payload #>> '{data,object,amount}')::numeric / 100), 0) AS total_payment_loss,
            COUNT(*)                                                                AS failed_charge_count
         FROM webhook_logs
         WHERE gateway_id     = $1
           AND event_type     = 'charge.failed'
           AND original_status = 'FAILED'
           AND received_at    >= NOW() - INTERVAL '${range}'
           AND (payload #>> '{data,object,amount}') IS NOT NULL`,
        [gateway_id],
      ),

      // 6. Delivery failure loss — infrastructure errors, safe to retry
      db.query(
        `SELECT
            COALESCE(SUM((payload #>> '{data,object,amount}')::numeric / 100), 0) AS total_delivery_loss,
            COUNT(*)                                                                AS delivery_failure_count
         FROM webhook_logs
         WHERE gateway_id     = $1
           AND original_status = 'FAILED'
           AND event_type     != 'charge.failed'
           AND received_at    >= NOW() - INTERVAL '${range}'
           AND (
               error_stack        ILIKE '%timeout%'
            OR error_stack        ILIKE '%connection%'
            OR error_stack        ILIKE '%ECONNREFUSED%'
            OR http_status_code = 500
           )`,
        [gateway_id],
      ),

      // 7. Recovery — successful auto / manual retries
      db.query(
        `SELECT
            COALESCE(SUM(amount), 0)                                               AS total_recovered,
            COUNT(*)                                                                AS recovery_count,
            COUNT(*) FILTER (WHERE recovery_type = 'AUTO_RETRY')                   AS auto_retry_count,
            COUNT(*) FILTER (WHERE recovery_type = 'MANUAL_RETRY')                 AS manual_retry_count
         FROM recovery_logs
         WHERE gateway_id   = $1
           AND recovered_at >= NOW() - INTERVAL '${range}'`,
        [gateway_id],
      ),
    ]);

    // ── Derive summary figures ─────────────────────────────────────
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
    console.error("❌ Analytics API error:", err.message);
    return res.status(500).json({ error: "Analytics query failed." });
  }
};

// ─── Webhook detail + RCA ─────────────────────────────────────────────────────

const getWebhookDetails = async (req, res) => {
  const { webhook_id } = req.params;

  try {
    const { rows } = await db.query(
      `SELECT
                w.*,
                r.issue_category,
                r.suggested_fix,
                r.severity AS rca_severity,

                CASE
                    WHEN w.event_type = 'charge.failed' THEN
                        CASE
                            WHEN w.payload #>> '{data,object,outcome,advice_code}' = 'do_not_retry'
                              OR w.payload #>> '{data,object,outcome,reason}'      IN ('fraudulent','do_not_honor')
                            THEN 'DO_NOT_RETRY'
                            WHEN w.payload #>> '{data,object,outcome,advice_code}' = 'try_again_later'
                            THEN 'RETRY'
                            ELSE 'RETRY_ONCE'
                        END
                END AS retry_posture,

                CASE
                    WHEN w.event_type = 'charge.failed'           THEN 'USER_FAILURE'
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
                     (r.decline_code IS NOT NULL
                      AND COALESCE(
                          w.payload #>> '{data,object,last_payment_error,decline_code}',
                          w.payload #>> '{data,object,outcome,reason}'
                      ) = r.decline_code)
                  OR (r.error_code IS NOT NULL
                      AND COALESCE(
                          w.payload #>> '{data,object,last_payment_error,code}',
                          w.payload #>> '{data,object,failure_code}'
                      ) = r.error_code)
                  OR (r.error_pattern IS NOT NULL
                      AND w.last_error_message ILIKE '%' || r.error_pattern || '%')
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
                         ELSE 1
                     END DESC,
                     r.id DESC
                 LIMIT 1
             ) r ON true
             WHERE w.id = $1`,
      [webhook_id],
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: "Webhook not found." });
    }

    const row = rows[0];
    const declineCode =
      row.payload?.data?.object?.outcome?.reason ??
      row.payload?.data?.object?.failure_code;

    const rcaHint =
      row.suggested_fix ?? getRcaHint(declineCode, row.last_error_message);

    return res.status(200).json({ ...row, rca_hint: rcaHint });
  } catch (err) {
    console.error("❌ RCA engine error:", err.message);
    return res.status(500).json({ error: "Failed to run RCA analysis." });
  }
};

// ─── Replay ───────────────────────────────────────────────────────────────────

const replayWebhook = async (req, res) => {
  const { id } = req.params;

  try {
    const { rows } = await db.query(
      `SELECT wl.*, g.webhook_secret, g.target_url AS webhook_url
             FROM webhook_logs wl
             JOIN gateways g ON wl.gateway_id = g.id
             WHERE wl.id = $1`,
      [id],
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: "Webhook not found." });
    }

    const webhook = rows[0];

    const obj          = webhook.payload?.data?.object ?? {};
    const retryPosture = getRetryPosture(
      obj?.outcome?.advice_code,
      obj?.outcome?.reason,
    );

    if (
      webhook.event_type === "charge.failed" &&
      retryPosture === "DO_NOT_RETRY"
    ) {
      return res.status(400).json({
        error:         "Replay blocked.",
        reason:        "Hard bank decline — replaying will yield the same result.",
        retry_posture: "DO_NOT_RETRY",
        decline_code:  obj?.outcome?.reason ?? null,
      });
    }

    if (!webhook.webhook_url) {
      return res
        .status(400)
        .json({ error: "No target URL configured for this gateway." });
    }

    const start         = Date.now();
    let httpResponse    = null;
    let replayStatus    = "SUCCESS";
    let errorStack      = null;

    try {
      httpResponse = await axios.post(webhook.webhook_url, webhook.payload, {
        headers: webhook.request_headers ?? {},
        timeout: REPLAY_TIMEOUT_MS,
      });
    } catch (err) {
      replayStatus = "FAILED";
      errorStack   = err.message;
    }

    if (replayStatus === "SUCCESS" && webhook.event_type === "charge.failed" && webhook.original_status === "FAILED") {
      const amount = (webhook.payload?.data?.object?.amount ?? 0) / 100;
      await db.query(
  `INSERT INTO recovery_logs
   (original_log_id, gateway_id, amount, recovery_type, attempt_number)
   VALUES ($1, $2, $3, 'MANUAL_RETRY', $4)
   ON CONFLICT (original_log_id) DO UPDATE
   SET attempt_number = EXCLUDED.attempt_number,
       recovered_at   = NOW()`,
  [webhook.id, webhook.gateway_id, amount, (webhook.retry_count ?? 0) + 1],
);
    }

    const latency = Date.now() - start;

    // Replay rows are new attempts — their original_status is whatever
    // the replay itself resolved to (SUCCESS or FAILED), not the parent's status.
    await db.query(
      `INSERT INTO webhook_logs (
                gateway_id,
                provider_event_id,
                event_type,
                status,
                original_status,
                http_status_code,
                latency_ms,
                payload,
                request_headers,
                error_stack,
                last_error_message,
                retry_count,
                replayed_from
            ) VALUES ($1,$2,$3,$4,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
      [
        webhook.gateway_id,
        `${webhook.provider_event_id}_replay_${Date.now()}`,
        webhook.event_type,
        replayStatus,         // $4 — used for both status and original_status
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
      message:       "Webhook replayed.",
      status:        replayStatus,
      latency_ms:    latency,
      retry_posture: retryPosture,
    });
  } catch (err) {
    console.error("❌ Replay error:", err.message);
    return res.status(500).json({ error: "Replay failed." });
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
      db.query("SELECT target_url FROM gateways WHERE id = $1", [gatewayId]),
      db.query(
        `SELECT id FROM webhook_logs
                 WHERE provider_event_id = $1 AND gateway_id = $2
                 ORDER BY received_at DESC LIMIT 1`,
        [event.id, gatewayId],
      ),
    ]);

    const targetUrl    = gatewayRes.rows[0]?.target_url;
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
    console.error("⚠️  Auto-retry scheduling failed (non-fatal):", err.message);
  }
};

const formatUsd = (value) => `$${value.toFixed(2)}`;

// ─── Exports ──────────────────────────────────────────────────────────────────

module.exports = {
  handleStripeWebhook,
  getGatewayAnalytics,
  getWebhookDetails,
  replayWebhook,
};