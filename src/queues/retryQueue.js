const { Queue, Worker } = require("bullmq");
const {
  notifyRetryExhausted,
  notifyLossRecovered,
  checkAndNotify,
} = require("../services/notificationService");
const axios = require("axios");
const redis = require("../config/redis");
const db = require("../config/db");
const promClient = require("prom-client");

// ─── Decision engine — all retry intelligence lives here ──────────────────────
const {
  makeRetryDecision,
  buildJourneyContext,
  updateJourneySubscriptionStatus,
  recordDeclineCodeOutcome,
  resolveDeclineProfile,
  computeSuccessProbability,
  DECLINE_CODE_PROFILES,
  SUCCESS_PROBABILITY_THRESHOLD,
} = require("../utils/retryDecisionEngine");

// ─────────────────────────────────────────────────────────────────────────────
// FIX 8 — PROMETHEUS OBSERVABILITY
// ─────────────────────────────────────────────────────────────────────────────
promClient.collectDefaultMetrics({ prefix: "webhook_retry_" });

const retryOutcomeCounter = new promClient.Counter({
  name: "webhook_retry_outcomes_total",
  help: "Total retry outcomes by status, attempt, and gateway",
  labelNames: ["status", "attempt_number", "gateway_id"],
});

const retryLatencyHistogram = new promClient.Histogram({
  name: "webhook_retry_latency_ms",
  help: "Latency of retry HTTP requests in milliseconds",
  buckets: [100, 500, 1000, 3000, 5000, 10000],
  labelNames: ["attempt_number"],
});

const circuitBreakerGauge = new promClient.Gauge({
  name: "webhook_circuit_breaker_state",
  help: "Circuit breaker state per gateway (0=closed, 1=open)",
  labelNames: ["gateway_id"],
});

const queueDepthGauge = new promClient.Gauge({
  name: "webhook_retry_queue_depth",
  help: "Jobs currently waiting or delayed in the retry queue",
});

const workerConcurrencyGauge = new promClient.Gauge({
  name: "webhook_retry_worker_concurrency",
  help: "Current concurrency level of the retry worker",
});

// Feeds success probability model in the decision engine (FIX 15)
const declineCodeOutcomeCounter = new promClient.Counter({
  name: "webhook_decline_code_outcomes_total",
  help: "Retry outcomes by decline code — feeds success probability model",
  labelNames: ["decline_code", "outcome", "card_country", "card_brand"],
});

const getMetrics = () => promClient.register.metrics();

// ─────────────────────────────────────────────────────────────────────────────
// HTTP ERROR CLASSIFICATION
// ─────────────────────────────────────────────────────────────────────────────
const classifyError = (err) => {
  const status = err.response?.status;
  if (!status) return { shouldRetry: true, reason: "network_timeout" };
  if (status === 429) return { shouldRetry: true, reason: "rate_limited" };
  if (status >= 400 && status < 500)
    return { shouldRetry: false, reason: `client_error_${status}` };
  if (status >= 500)
    return { shouldRetry: true, reason: `server_error_${status}` };
  return { shouldRetry: true, reason: "unknown" };
};

// ─────────────────────────────────────────────────────────────────────────────
// CIRCUIT BREAKER — per gateway in Redis
// ─────────────────────────────────────────────────────────────────────────────
const CIRCUIT_BREAKER_THRESHOLD = 5;
const CIRCUIT_BREAKER_WINDOW = 10 * 60; // seconds
const CIRCUIT_BREAKER_COOLDOWN = 5 * 60 * 1000; // ms

const isCircuitOpen = async (gatewayId) => {
  const open = !!(await redis.get(`circuit:${gatewayId}:open`));
  circuitBreakerGauge.set({ gateway_id: String(gatewayId) }, open ? 1 : 0);
  return open;
};

const recordFailure = async (gatewayId) => {
  const key = `circuit:${gatewayId}:failures`;
  const count = await redis.incr(key);
  await redis.expire(key, CIRCUIT_BREAKER_WINDOW);
  if (count >= CIRCUIT_BREAKER_THRESHOLD) {
    await redis.set(
      `circuit:${gatewayId}:open`,
      "1",
      "EX",
      CIRCUIT_BREAKER_COOLDOWN / 1000,
    );
    circuitBreakerGauge.set({ gateway_id: String(gatewayId) }, 1);
    console.warn(`⚡ Circuit OPEN for gateway ${gatewayId}`);
  }
};

const resetCircuit = async (gatewayId) => {
  await redis.del(`circuit:${gatewayId}:failures`);
  await redis.del(`circuit:${gatewayId}:open`);
  circuitBreakerGauge.set({ gateway_id: String(gatewayId) }, 0);
};

// ─────────────────────────────────────────────────────────────────────────────
// FIX 9 — PRIORITY QUEUING (urgency-aware via FIX 16)
// ─────────────────────────────────────────────────────────────────────────────
const EVENT_PRIORITY = {
  "charge.failed": 10,
  "payment_intent.payment_failed": 10,
  "charge.dispute.created": 7,
  "customer.subscription.deleted": 5,
};
const URGENCY_PRIORITY_BONUS = { CRITICAL: 5, HIGH: 3, MEDIUM: 1, LOW: 0 };

const getJobPriority = (eventType, urgency = "LOW") => {
  const base = EVENT_PRIORITY[eventType] ?? 1;
  const bonus = URGENCY_PRIORITY_BONUS[urgency] ?? 0;
  return Math.min(base + bonus, 20);
};

// ─────────────────────────────────────────────────────────────────────────────
// QUEUES
// ─────────────────────────────────────────────────────────────────────────────
const retryQueue = new Queue("webhook-retry", {
  connection: redis,
  defaultJobOptions: { removeOnComplete: 100, removeOnFail: 200 },
});

const deadLetterQueue = new Queue("webhook-dlq", {
  connection: redis,
  defaultJobOptions: { removeOnComplete: false, removeOnFail: false },
});

setInterval(async () => {
  try {
    const depth =
      (await retryQueue.getWaitingCount()) +
      (await retryQueue.getDelayedCount());
    queueDepthGauge.set(depth);
  } catch (_) {}
}, 15_000);

// ─────────────────────────────────────────────────────────────────────────────
// FIX 11 — DB FALLBACK when Redis is down
// ─────────────────────────────────────────────────────────────────────────────
const saveRetryFallbackToDB = async ({
  webhookLogId,
  gatewayId,
  targetUrl,
  attemptNumber,
  scheduledAt,
}) => {
  try {
    await db.query(
      `INSERT INTO retry_fallback_queue
                 (webhook_log_id, gateway_id, target_url, attempt_number, scheduled_at, status)
             VALUES ($1,$2,$3,$4,$5,'PENDING')
             ON CONFLICT (webhook_log_id, attempt_number) DO NOTHING`,
      [webhookLogId, gatewayId, targetUrl, attemptNumber, scheduledAt],
    );
    console.warn(
      `💾 Fallback: retry #${attemptNumber} for ${webhookLogId} saved to DB`,
    );
  } catch (dbErr) {
    console.error(
      `🚨 CRITICAL: Redis AND DB both failed for ${webhookLogId}`,
      dbErr.message,
    );
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// IDEMPOTENCY LOCK
// ─────────────────────────────────────────────────────────────────────────────
const acquireProcessingLock = async (webhookLogId, attemptNumber) => {
  const lockKey = `lock:retry:${webhookLogId}:attempt:${attemptNumber}`;
  const acquired = await redis.set(lockKey, "1", "EX", 900, "NX");
  return { acquired: acquired === "OK", lockKey };
};

const releaseProcessingLock = async (lockKey) => redis.del(lockKey);

// ─────────────────────────────────────────────────────────────────────────────
// FIX 10 — ADAPTIVE BACKPRESSURE
// ─────────────────────────────────────────────────────────────────────────────
const MIN_CONCURRENCY = 5;
const MAX_CONCURRENCY = 20;
const BACKPRESSURE_CHECK_MS = 20_000;
const BACKPRESSURE_HIGH_WATER = 100;
const BACKPRESSURE_LOW_WATER = 20;

let currentConcurrency = MIN_CONCURRENCY;

const adjustConcurrency = async (worker) => {
  try {
    const depth =
      (await retryQueue.getWaitingCount()) +
      (await retryQueue.getDelayedCount());
    let next = currentConcurrency;

    if (
      depth > BACKPRESSURE_HIGH_WATER &&
      currentConcurrency < MAX_CONCURRENCY
    ) {
      next = Math.min(currentConcurrency + 5, MAX_CONCURRENCY);
      console.warn(
        `📈 Backpressure: queue=${depth} → concurrency ${currentConcurrency}→${next}`,
      );
    } else if (
      depth < BACKPRESSURE_LOW_WATER &&
      currentConcurrency > MIN_CONCURRENCY
    ) {
      next = Math.max(currentConcurrency - 5, MIN_CONCURRENCY);
      console.log(
        `📉 Backpressure: queue=${depth} → concurrency ${currentConcurrency}→${next}`,
      );
    }

    if (next !== currentConcurrency) {
      currentConcurrency = next;
      await worker.setConcurrency(next);
      workerConcurrencyGauge.set(next);
    }
  } catch (_) {}
};

// ─────────────────────────────────────────────────────────────────────────────
// SCHEDULE RETRY
// Calls makeRetryDecision() from the engine — no decision logic inline.
// ─────────────────────────────────────────────────────────────────────────────
const MAX_ATTEMPTS = 4;

const scheduleRetry = async ({
  webhookLogId,
  gatewayId,
  payload,
  targetUrl,
  attemptNumber = 1,
}) => {
  const slimPayload = {
    id: payload?.id,
    type: payload?.type,
    data: {
      object: {
        amount: payload?.data?.object?.amount,
        currency: payload?.data?.object?.currency,
        customer: payload?.data?.object?.customer,
        invoice: payload?.data?.object?.invoice,
        outcome: payload?.data?.object?.outcome,
        payment_method_details: payload?.data?.object?.payment_method_details,
      },
    },
  };

  // Hard cap — exhaust before even consulting the engine
  if (attemptNumber > MAX_ATTEMPTS) {
    await db.query(
      `UPDATE webhook_logs
             SET status             = 'FAILED',
                 last_error_message = 'retry_exhausted',
                 error_stack        = COALESCE(error_stack, '') || ' | Auto-retry exhausted after 4 attempts.'
             WHERE id = $1`,
      [webhookLogId],
    );
    await deadLetterQueue.add(
      "exhausted-webhook",
      { webhookLogId, gatewayId, targetUrl, slimPayload },
      { jobId: `dlq-${webhookLogId}` },
    );
    retryOutcomeCounter.inc({
      status: "EXHAUSTED",
      attempt_number: String(attemptNumber),
      gateway_id: String(gatewayId),
    });

    const logRes = await db.query(
      `SELECT payload FROM webhook_logs WHERE id = $1`,
      [webhookLogId],
    );
    const logPayload = logRes.rows[0]?.payload;
    await notifyRetryExhausted({
      gateway_id: gatewayId,
      webhookLogId,
      eventId: logPayload?.id,
      amount: logPayload?.data?.object?.amount
        ? logPayload.data.object.amount / 100
        : 0,
      declineReason: logPayload?.data?.object?.outcome?.reason,
    });
    return;
  }

  // Ask the engine for the full decision
  const declineCode =
    slimPayload?.data?.object?.outcome?.network_decline_code ??
    slimPayload?.data?.object?.outcome?.reason;

  const decision = await makeRetryDecision({
    gatewayId,
    declineCode,
    attemptNumber,
    payload: slimPayload,
  });

  if (decision.action !== "RETRY") {
    // Engine says stop — log and DLQ
    console.warn(
      `🚫 scheduleRetry: engine returned ${decision.action} for ${webhookLogId} ` +
        `[code=${declineCode}] — ${decision.reason}`,
    );
    await deadLetterQueue.add(
      `${decision.action.toLowerCase()}-webhook`,
      { webhookLogId, gatewayId, targetUrl, declineCode, ...decision },
      { jobId: `dlq-${decision.action}-${webhookLogId}-${Date.now()}` },
    );
    return;
  }

  const { delayMs, priority } = decision;
  const scheduledAt = new Date(Date.now() + delayMs);

  try {
    await retryQueue.add(
      "retry-webhook",
      {
        webhookLogId,
        gatewayId,
        slimPayload,
        targetUrl,
        attemptNumber,
        declineCode,
      },
      {
        delay: delayMs,
        priority,
        jobId: `retry-${webhookLogId}-attempt-${attemptNumber}`,
      },
    );
    console.log(
      `⏳ Retry #${attemptNumber} for ${webhookLogId} in ${Math.round(delayMs / 1000)}s ` +
        `[priority=${priority}, urgency=${decision.journeyContext.urgency}, code=${declineCode ?? "unknown"}]`,
    );
  } catch (redisErr) {
    console.error(`⚠️ Redis down — falling back to DB`, redisErr.message);
    await saveRetryFallbackToDB({
      webhookLogId,
      gatewayId,
      targetUrl,
      attemptNumber,
      scheduledAt,
    });
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// WORKER
// Calls makeRetryDecision() for the full gate check before each HTTP attempt.
// ─────────────────────────────────────────────────────────────────────────────
const retryWorker = new Worker(
  "webhook-retry",
  async (job) => {
    const {
      webhookLogId,
      gatewayId,
      slimPayload,
      targetUrl,
      attemptNumber,
      declineCode,
    } = job.data;

    // ── Idempotency lock ──────────────────────────────────────────────────
    const { acquired, lockKey } = await acquireProcessingLock(
      webhookLogId,
      attemptNumber,
    );
    if (!acquired) {
      console.warn(
        `⚠️ Duplicate — skipping retry #${attemptNumber} for ${webhookLogId}`,
      );
      return { status: "SKIPPED_DUPLICATE", attemptNumber };
    }

    try {
      // ── Circuit breaker ───────────────────────────────────────────────
      if (await isCircuitOpen(gatewayId)) {
        console.warn(`⚡ Circuit OPEN for gateway ${gatewayId} — rescheduling`);
        await retryQueue.add("retry-webhook", job.data, {
          delay: CIRCUIT_BREAKER_COOLDOWN,
          priority: getJobPriority(slimPayload?.type),
          jobId: `retry-${webhookLogId}-attempt-${attemptNumber}-cb-${Date.now()}`,
        });
        return { status: "CIRCUIT_OPEN_RESCHEDULED", attemptNumber };
      }

      // ── Trial / subscription gate ─────────────────────────────────────
      const userCheck = await db.query(
        `SELECT u.subscription_status, u.trial_ends_at, u.is_premium
                 FROM gateways g JOIN users u ON g.user_id = u.id WHERE g.id = $1`,
        [gatewayId],
      );
      const user = userCheck.rows[0];
      const isTrialActive =
        user.is_premium ||
        (user.subscription_status === "trialing" &&
          new Date() < new Date(user.trial_ends_at));

      if (!isTrialActive) {
        retryOutcomeCounter.inc({
          status: "TRIAL_EXPIRED",
          attempt_number: String(attemptNumber),
          gateway_id: String(gatewayId),
        });
        return { status: "PAUSED_TRIAL_EXPIRED", attemptNumber };
      }

      // ── Fetch full payload (DB preferred; slim fallback) ──────────────
      let fullPayload = slimPayload;
      try {
        const logRow = await db.query(
          `SELECT payload FROM webhook_logs WHERE id = $1`,
          [webhookLogId],
        );
        if (logRow.rows[0]?.payload) fullPayload = logRow.rows[0].payload;
      } catch (_) {
        console.warn(
          `⚠️ DB down for payload fetch — using slimPayload for ${webhookLogId}`,
        );
      }

      // ── Decision engine — single authoritative gate ───────────────────
      const decision = await makeRetryDecision({
        gatewayId,
        declineCode,
        attemptNumber,
        payload: fullPayload,
      });

      const { action, profile, journeyContext, successProbability } = decision;

      // ── Handle non-RETRY outcomes from the engine ─────────────────────
      if (action !== "RETRY") {
        const logLabel =
          {
            LOW_PROBABILITY_STOP: `📉 Probability too low (${(successProbability * 100).toFixed(1)}%)`,
            HARD_STOP: `🚫 HARD_FAIL (${declineCode})`,
            ACTION_REQUIRED: `🔐 ACTION_REQUIRED (${declineCode})`,
          }[action] ?? `🚫 ${action}`;

        console.warn(`${logLabel} for ${webhookLogId} — ${decision.reason}`);

        // Prometheus
        declineCodeOutcomeCounter.inc({
          decline_code: declineCode ?? "unknown",
          outcome: action,
          card_country: journeyContext.cardCountry,
          card_brand: journeyContext.cardBrand,
        });
        retryOutcomeCounter.inc({
          status: action,
          attempt_number: String(attemptNumber),
          gateway_id: String(gatewayId),
        });

        // DB update
        try {
          await db.query(
            `UPDATE webhook_logs
                         SET status             = 'FAILED',
                             last_error_message = $2,
                             error_stack        = error_stack || $3
                         WHERE id = $1`,
            [
              webhookLogId,
              `${action.toLowerCase()}_${declineCode ?? "unknown"}`,
              decision.userAction
                ? ` | User action required: ${decision.userAction}`
                : "",
            ],
          );
        } catch (_) {}

        await deadLetterQueue.add(
          `${action.toLowerCase()}-webhook`,
          {
            webhookLogId,
            gatewayId,
            targetUrl,
            declineCode,
            action,
            reason: decision.reason,
            userAction: decision.userAction,
            successProbability,
            journeyContext,
          },
          { jobId: `dlq-${action}-${webhookLogId}-${Date.now()}` },
        );

        await recordDeclineCodeOutcome(declineCode, "HARD_FAIL");
        return { status: `FAILED_${action}`, attemptNumber, declineCode };
      }

      // ── HTTP request ──────────────────────────────────────────────────
      console.log(
        `🔄 Retry #${attemptNumber} for ${webhookLogId} ` +
          `[code=${declineCode}, prob=${(successProbability * 100).toFixed(1)}%, ` +
          `urgency=${journeyContext.urgency}]`,
      );

      const start = Date.now();
      const response = await axios.post(targetUrl, fullPayload, {
        timeout: 10_000,
        headers: { "Content-Type": "application/json" },
      });
      const latency = Date.now() - start;

      // ── Success path ──────────────────────────────────────────────────
      retryLatencyHistogram.observe(
        { attempt_number: String(attemptNumber) },
        latency,
      );
      retryOutcomeCounter.inc({
        status: "SUCCESS",
        attempt_number: String(attemptNumber),
        gateway_id: String(gatewayId),
      });
      declineCodeOutcomeCounter.inc({
        decline_code: declineCode ?? "unknown",
        outcome: "SUCCESS",
        card_country: journeyContext.cardCountry,
        card_brand: journeyContext.cardBrand,
      });

      await resetCircuit(gatewayId);
      await recordDeclineCodeOutcome(declineCode, "SUCCESS"); // feeds probability model

      try {
        await db.query(
          `INSERT INTO webhook_logs (
                         gateway_id, provider_event_id, event_type, status,
                         http_status_code, latency_ms, payload, error_stack,
                         last_error_message, retry_count, replayed_from
                     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
          [
            gatewayId,
            `${fullPayload.id}_auto_retry_${attemptNumber}`,
            fullPayload.type,
            "SUCCESS",
            response.status,
            latency,
            JSON.stringify(fullPayload),
            null,
            null,
            attemptNumber,
            webhookLogId,
          ],
        );

        const amount = fullPayload?.data?.object?.amount
          ? fullPayload.data.object.amount / 100
          : null;

        if (amount) {
          await db.query(
            `INSERT INTO recovery_logs
                             (original_log_id, gateway_id, amount, recovery_type, attempt_number)
                         VALUES ($1,$2,$3,$4,$5)`,
            [webhookLogId, gatewayId, amount, "AUTO_RETRY", attemptNumber],
          );
        }

        await notifyLossRecovered({
          gateway_id: gatewayId,
          amount,
          attemptNumber,
        });
        await checkAndNotify({ gateway_id: gatewayId });
      } catch (dbWriteErr) {
        console.error(
          `⚠️ DB write failed after successful retry ${webhookLogId}:`,
          dbWriteErr.message,
        );
      }

      console.log(
        `✅ Retry #${attemptNumber} succeeded for ${webhookLogId} (${latency}ms)`,
      );
      return { status: "SUCCESS", attemptNumber, httpStatus: response.status };
    } catch (err) {
      // ── HTTP failure ──────────────────────────────────────────────────
      const { shouldRetry, reason } = classifyError(err);

      console.log(
        `❌ Retry #${attemptNumber} failed for ${webhookLogId} — ${reason}`,
      );

      retryOutcomeCounter.inc({
        status: "FAILED",
        attempt_number: String(attemptNumber),
        gateway_id: String(gatewayId),
      });
      await recordFailure(gatewayId);
      await recordDeclineCodeOutcome(declineCode, "FAILED");

      if (!shouldRetry) {
        console.warn(`🚫 Non-retryable (${reason}) → DLQ: ${webhookLogId}`);
        try {
          await db.query(
            `UPDATE webhook_logs
                         SET status = 'FAILED', last_error_message = $2
                         WHERE id = $1`,
            [webhookLogId, `non_retryable_${reason}`],
          );
        } catch (_) {}
        await deadLetterQueue.add(
          "non-retryable-webhook",
          {
            webhookLogId,
            gatewayId,
            targetUrl,
            reason,
            httpStatus: err.response?.status || 0,
          },
          { jobId: `dlq-nonretry-${webhookLogId}-${Date.now()}` },
        );
        retryOutcomeCounter.inc({
          status: "NON_RETRYABLE",
          attempt_number: String(attemptNumber),
          gateway_id: String(gatewayId),
        });
        return { status: "FAILED_NON_RETRYABLE", attemptNumber, reason };
      }

      // Re-schedule via scheduleRetry so the engine gates the next attempt too
      await scheduleRetry({
        webhookLogId,
        gatewayId,
        payload: slimPayload,
        targetUrl,
        attemptNumber: attemptNumber + 1,
      });
      return {
        status: "FAILED_WILL_RETRY",
        attemptNumber,
        httpStatus: err.response?.status || 0,
      };
    } finally {
      await releaseProcessingLock(lockKey);
    }
  },
  { connection: redis, concurrency: MIN_CONCURRENCY },
);

// FIX 10 — adaptive backpressure
retryWorker.on("ready", () => {
  workerConcurrencyGauge.set(MIN_CONCURRENCY);
  setInterval(() => adjustConcurrency(retryWorker), BACKPRESSURE_CHECK_MS);
  console.log(
    "🚀 Retry worker online — adaptive backpressure + smart decline routing active",
  );
});

// ─────────────────────────────────────────────────────────────────────────────
// DLQ WORKER
// ─────────────────────────────────────────────────────────────────────────────
const dlqWorker = new Worker(
  "webhook-dlq",
  async (job) => {
    const {
      webhookLogId,
      gatewayId,
      reason,
      action,
      userAction,
      journeyContext,
    } = job.data;
    console.error(
      `📬 DLQ — log: ${webhookLogId}, gateway: ${gatewayId}, ` +
        `action: ${action ?? "exhausted"}, reason: ${reason ?? "retry_exhausted"}`,
    );
    if (userAction) {
      console.error(`📬 DLQ — user action required: ${userAction}`);
    }
    if (journeyContext?.isSubscriptionAtRisk) {
      console.error(
        `🚨 DLQ — SUBSCRIPTION AT RISK for gateway ${gatewayId} — escalate!`,
      );
      // TODO: await notifyAdminSlack({ webhookLogId, gatewayId, reason, urgency: 'CRITICAL' });
    }
  },
  { connection: redis, concurrency: 2 },
);

// ─────────────────────────────────────────────────────────────────────────────
// WORKER EVENT LISTENERS
// ─────────────────────────────────────────────────────────────────────────────
retryWorker.on("completed", (job, result) => {
  console.log(
    `✅ Job ${job.id} done — attempt #${result.attemptNumber} → ${result.status}`,
  );
});
retryWorker.on("failed", (job, err) => {
  console.error(`🔥 Job ${job.id} crashed — ${err.message}`);
  retryOutcomeCounter.inc({
    status: "WORKER_CRASH",
    attempt_number: "unknown",
    gateway_id: "unknown",
  });
});
dlqWorker.on("failed", (job, err) => {
  console.error(`🔥 DLQ Job ${job.id} crashed — ${err.message}`);
});

// ─────────────────────────────────────────────────────────────────────────────
// EXPORTS
// ─────────────────────────────────────────────────────────────────────────────
module.exports = {
  retryQueue,
  deadLetterQueue,
  scheduleRetry,
  getMetrics,
  updateJourneySubscriptionStatus,
  buildJourneyContext,
  resolveDeclineProfile,
  computeSuccessProbability,
  DECLINE_CODE_PROFILES,
};
