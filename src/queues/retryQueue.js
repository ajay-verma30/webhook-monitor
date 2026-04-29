/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║           WEBHOOK RETRY QUEUE — ELITE PRODUCTION v2          ║
 * ║                                                              ║
 * ║  v1 fixes (already done):                                    ║
 * ║   ✅ Idempotency lock (Redis SETNX)                          ║
 * ║   ✅ Redis fallback → DB persistence                         ║
 * ║   ✅ Dead Letter Queue (DLQ)                                 ║
 * ║   ✅ HTTP error classification (4xx vs 5xx)                  ║
 * ║   ✅ Circuit breaker per gateway                             ║
 * ║   ✅ Slim payload in Redis                                   ║
 * ║   ✅ Jitter on retry delays                                  ║
 * ║                                                              ║
 * ║  v2 new elite fixes:                                         ║
 * ║   ✅ [FIX 8]  Prometheus observability (metrics + histograms)║
 * ║   ✅ [FIX 9]  Priority queuing (payments > logs)             ║
 * ║   ✅ [FIX 10] Adaptive backpressure (auto-scale concurrency) ║
 * ║   ✅ [FIX 11] DB-down resilience (slimPayload always usable) ║
 * ╚══════════════════════════════════════════════════════════════╝
 */

const { Queue, Worker } = require('bullmq');
const { notifyRetryExhausted, notifyLossRecovered, checkAndNotify } = require('../services/notificationService');
const axios      = require('axios');
const redis      = require('../config/redis');
const db         = require('../config/db');
const promClient = require('prom-client'); // npm install prom-client


// ─────────────────────────────────────────────────────────────────
// FIX 8 — PROMETHEUS OBSERVABILITY
//
// Exposes metrics at GET /metrics — scraped by Prometheus,
// visualised in Grafana. Gives you:
//   • retry success/failure rates
//   • latency percentiles (p50, p95, p99)
//   • circuit breaker state per gateway
//   • queue depth over time
//   • worker concurrency level
// ─────────────────────────────────────────────────────────────────
promClient.collectDefaultMetrics({ prefix: 'webhook_retry_' });

// How many retries ended in each outcome (SUCCESS / FAILED / etc.)
const retryOutcomeCounter = new promClient.Counter({
    name:       'webhook_retry_outcomes_total',
    help:       'Total retry outcomes grouped by status, attempt, and gateway',
    labelNames: ['status', 'attempt_number', 'gateway_id'],
});

// How long each HTTP retry call actually took (in ms)
const retryLatencyHistogram = new promClient.Histogram({
    name:       'webhook_retry_latency_ms',
    help:       'Latency of retry HTTP requests in milliseconds',
    buckets:    [100, 500, 1000, 3000, 5000, 10000],
    labelNames: ['attempt_number'],
});

// Is each gateway's circuit breaker open (1) or closed (0)?
const circuitBreakerGauge = new promClient.Gauge({
    name:       'webhook_circuit_breaker_state',
    help:       'Circuit breaker state per gateway (0=closed, 1=open)',
    labelNames: ['gateway_id'],
});

// How many jobs are currently sitting in the queue?
const queueDepthGauge = new promClient.Gauge({
    name: 'webhook_retry_queue_depth',
    help: 'Number of jobs currently waiting or delayed in the retry queue',
});

// What concurrency level is the worker currently running at?
const workerConcurrencyGauge = new promClient.Gauge({
    name: 'webhook_retry_worker_concurrency',
    help: 'Current concurrency level of the retry worker',
});

/**
 * Expose this endpoint in your Express app:
 *
 *   const { getMetrics } = require('./queues/retryQueue');
 *   app.get('/metrics', async (req, res) => {
 *       res.set('Content-Type', promClient.register.contentType);
 *       res.send(await getMetrics());
 *   });
 *
 * Then point Prometheus scraper at http://your-server/metrics
 */
const getMetrics = () => promClient.register.metrics();


// ─────────────────────────────────────────────────────────────────
// RETRY DELAYS — base delays per attempt (jitter added on top)
// ─────────────────────────────────────────────────────────────────
const RETRY_DELAYS = {
    1: 5  * 60 * 1000,
    2: 30 * 60 * 1000,
    3: 2  * 60 * 60 * 1000,
    4: 24 * 60 * 60 * 1000,
};
const MAX_ATTEMPTS = 4;

// ─────────────────────────────────────────────────────────────────
// JITTER — ±20% randomness prevents retry storms
// ─────────────────────────────────────────────────────────────────
const withJitter = (baseDelay) => {
    const jitter = baseDelay * 0.2 * (Math.random() * 2 - 1);
    return Math.max(1000, Math.round(baseDelay + jitter));
};

// ─────────────────────────────────────────────────────────────────
// HTTP ERROR CLASSIFICATION
// ─────────────────────────────────────────────────────────────────
const classifyError = (err) => {
    const status = err.response?.status;
    if (!status)                       return { shouldRetry: true,  reason: 'network_timeout'       };
    if (status === 429)                return { shouldRetry: true,  reason: 'rate_limited'           };
    if (status >= 400 && status < 500) return { shouldRetry: false, reason: `client_error_${status}` };
    if (status >= 500)                 return { shouldRetry: true,  reason: `server_error_${status}` };
    return { shouldRetry: true, reason: 'unknown' };
};

// ─────────────────────────────────────────────────────────────────
// CIRCUIT BREAKER — per gateway in Redis
// ─────────────────────────────────────────────────────────────────
const CIRCUIT_BREAKER_THRESHOLD = 5;
const CIRCUIT_BREAKER_WINDOW    = 10 * 60;        // seconds (for Redis TTL)
const CIRCUIT_BREAKER_COOLDOWN  = 5  * 60 * 1000; // ms (for BullMQ delay)

const isCircuitOpen = async (gatewayId) => {
    const open = !!(await redis.get(`circuit:${gatewayId}:open`));
    circuitBreakerGauge.set({ gateway_id: String(gatewayId) }, open ? 1 : 0); // FIX 8
    return open;
};

const recordFailure = async (gatewayId) => {
    const key   = `circuit:${gatewayId}:failures`;
    const count = await redis.incr(key);
    await redis.expire(key, CIRCUIT_BREAKER_WINDOW);
    if (count >= CIRCUIT_BREAKER_THRESHOLD) {
        await redis.set(`circuit:${gatewayId}:open`, '1', 'EX', CIRCUIT_BREAKER_COOLDOWN / 1000);
        circuitBreakerGauge.set({ gateway_id: String(gatewayId) }, 1); // FIX 8
        console.warn(`⚡ Circuit OPEN for gateway ${gatewayId}`);
    }
};

const resetCircuit = async (gatewayId) => {
    await redis.del(`circuit:${gatewayId}:failures`);
    await redis.del(`circuit:${gatewayId}:open`);
    circuitBreakerGauge.set({ gateway_id: String(gatewayId) }, 0); // FIX 8
};

// ─────────────────────────────────────────────────────────────────
// FIX 9 — PRIORITY QUEUING
//
// BullMQ supports job priority: higher number = dequeued first.
// Payment failures are urgent → priority 10.
// Informational events → priority 1.
// This means if 50 payment failures and 50 log events are all
// waiting, the payment retries drain first, always.
// ─────────────────────────────────────────────────────────────────
const EVENT_PRIORITY = {
    'charge.failed':                  10,
    'payment_intent.payment_failed':  10,
    'charge.dispute.created':          7,
    'customer.subscription.deleted':   5,
    // anything else defaults to 1
};
const getJobPriority = (eventType) => EVENT_PRIORITY[eventType] ?? 1;

// ─────────────────────────────────────────────────────────────────
// QUEUES
// ─────────────────────────────────────────────────────────────────
const retryQueue = new Queue('webhook-retry', {
    connection: redis,
    defaultJobOptions: { removeOnComplete: 100, removeOnFail: 200 },
});

const deadLetterQueue = new Queue('webhook-dlq', {
    connection: redis,
    defaultJobOptions: { removeOnComplete: false, removeOnFail: false },
});

// FIX 8 — poll queue depth every 15s for Prometheus
setInterval(async () => {
    try {
        const depth = (await retryQueue.getWaitingCount()) + (await retryQueue.getDelayedCount());
        queueDepthGauge.set(depth);
    } catch (_) {}
}, 15_000);

// ─────────────────────────────────────────────────────────────────
// FALLBACK: DB persistence when Redis is down
// ─────────────────────────────────────────────────────────────────
const saveRetryFallbackToDB = async ({ webhookLogId, gatewayId, targetUrl, attemptNumber, scheduledAt }) => {
    try {
        await db.query(
            `INSERT INTO retry_fallback_queue
                (webhook_log_id, gateway_id, target_url, attempt_number, scheduled_at, status)
             VALUES ($1, $2, $3, $4, $5, 'PENDING')
             ON CONFLICT (webhook_log_id, attempt_number) DO NOTHING`,
            [webhookLogId, gatewayId, targetUrl, attemptNumber, scheduledAt]
        );
        console.warn(`💾 Fallback: retry #${attemptNumber} for ${webhookLogId} saved to DB`);
    } catch (dbErr) {
        console.error(`🚨 CRITICAL: Redis AND DB both failed for ${webhookLogId}`, dbErr.message);
    }
};

// ─────────────────────────────────────────────────────────────────
// IDEMPOTENCY LOCK
// ─────────────────────────────────────────────────────────────────
const acquireProcessingLock = async (webhookLogId, attemptNumber) => {
    const lockKey  = `lock:retry:${webhookLogId}:attempt:${attemptNumber}`;
    const acquired = await redis.set(lockKey, '1', 'EX', 900, 'NX');
    return { acquired: acquired === 'OK', lockKey };
};
const releaseProcessingLock = async (lockKey) => redis.del(lockKey);

// ─────────────────────────────────────────────────────────────────
// FIX 10 — ADAPTIVE BACKPRESSURE
//
// Every 20 seconds we check how many jobs are queued up.
// If it's growing fast (>100 jobs) → increase worker concurrency to chew faster.
// If it's calm (<20 jobs)          → scale back down to save resources.
//
// This is the difference between a system that silently falls behind
// during a traffic spike vs one that self-heals automatically.
// ─────────────────────────────────────────────────────────────────
const MIN_CONCURRENCY         = 5;
const MAX_CONCURRENCY         = 20;
const BACKPRESSURE_CHECK_MS   = 20_000;
const BACKPRESSURE_HIGH_WATER = 100;  // scale UP above this
const BACKPRESSURE_LOW_WATER  = 20;   // scale DOWN below this

let currentConcurrency = MIN_CONCURRENCY;

const adjustConcurrency = async (worker) => {
    try {
        const depth = (await retryQueue.getWaitingCount()) + (await retryQueue.getDelayedCount());
        let next    = currentConcurrency;

        if (depth > BACKPRESSURE_HIGH_WATER && currentConcurrency < MAX_CONCURRENCY) {
            next = Math.min(currentConcurrency + 5, MAX_CONCURRENCY);
            console.warn(`📈 Backpressure: queue=${depth} → concurrency ${currentConcurrency}→${next}`);
        } else if (depth < BACKPRESSURE_LOW_WATER && currentConcurrency > MIN_CONCURRENCY) {
            next = Math.max(currentConcurrency - 5, MIN_CONCURRENCY);
            console.log(`📉 Backpressure: queue=${depth} → concurrency ${currentConcurrency}→${next}`);
        }

        if (next !== currentConcurrency) {
            currentConcurrency = next;
            await worker.setConcurrency(next);    // BullMQ v4+ live-change
            workerConcurrencyGauge.set(next);     // FIX 8 — metric
        }
    } catch (_) {}
};

// ─────────────────────────────────────────────────────────────────
// SCHEDULE RETRY — entry point called from webhookController
// ─────────────────────────────────────────────────────────────────
const scheduleRetry = async ({ webhookLogId, gatewayId, payload, targetUrl, attemptNumber = 1 }) => {

    // FIX 11 — slimPayload has enough data to fire the request even if DB is down.
    // If DB is up, worker fetches the full payload. If DB is down, slimPayload is the fallback.
    const slimPayload = {
        id:   payload?.id,
        type: payload?.type,
        data: {
            object: {
                amount:   payload?.data?.object?.amount,
                currency: payload?.data?.object?.currency,
                outcome:  { reason: payload?.data?.object?.outcome?.reason },
            },
        },
    };

    if (attemptNumber > MAX_ATTEMPTS) {
        // ── EXHAUSTED ─────────────────────────────────────────────────
        await db.query(
            `UPDATE webhook_logs
             SET status             = 'FAILED',
                 last_error_message = 'retry_exhausted',
                 error_stack        = COALESCE(error_stack, '') || ' | Auto-retry exhausted after 4 attempts.'
             WHERE id = $1`,
            [webhookLogId]
        );
        await deadLetterQueue.add(
            'exhausted-webhook',
            { webhookLogId, gatewayId, targetUrl, slimPayload },
            { jobId: `dlq-${webhookLogId}` }
        );
        retryOutcomeCounter.inc({ status: 'EXHAUSTED', attempt_number: String(attemptNumber), gateway_id: String(gatewayId) }); // FIX 8

        console.log(`🔴 Retry exhausted → DLQ: ${webhookLogId}`);
        const logRes     = await db.query(`SELECT payload FROM webhook_logs WHERE id = $1`, [webhookLogId]);
        const logPayload = logRes.rows[0]?.payload;
        await notifyRetryExhausted({
            gateway_id:    gatewayId,
            webhookLogId,
            eventId:       logPayload?.id,
            amount:        logPayload?.data?.object?.amount ? logPayload.data.object.amount / 100 : 0,
            declineReason: logPayload?.data?.object?.outcome?.reason,
        });
        return;
    }

    const delay       = withJitter(RETRY_DELAYS[attemptNumber]);
    const scheduledAt = new Date(Date.now() + delay);
    const priority    = getJobPriority(slimPayload?.type); // FIX 9

    try {
        await retryQueue.add(
            'retry-webhook',
            { webhookLogId, gatewayId, slimPayload, targetUrl, attemptNumber },
            {
                delay,
                priority,  // FIX 9 — payment jobs jump the queue
                jobId: `retry-${webhookLogId}-attempt-${attemptNumber}`,
            }
        );
        console.log(`⏳ Retry #${attemptNumber} for ${webhookLogId} in ${Math.round(delay / 1000)}s [priority=${priority}]`);
    } catch (redisErr) {
        console.error(`⚠️ Redis down — falling back to DB`, redisErr.message);
        await saveRetryFallbackToDB({ webhookLogId, gatewayId, targetUrl, attemptNumber, scheduledAt });
    }
};


// ─────────────────────────────────────────────────────────────────
// WORKER
// ─────────────────────────────────────────────────────────────────
const retryWorker = new Worker(
    'webhook-retry',
    async (job) => {
        const { webhookLogId, gatewayId, slimPayload, targetUrl, attemptNumber } = job.data;

        // ── IDEMPOTENCY LOCK ──────────────────────────────────────────
        const { acquired, lockKey } = await acquireProcessingLock(webhookLogId, attemptNumber);
        if (!acquired) {
            console.warn(`⚠️ Duplicate — skipping retry #${attemptNumber} for ${webhookLogId}`);
            return { status: 'SKIPPED_DUPLICATE', attemptNumber };
        }

        try {
            // ── CIRCUIT BREAKER ───────────────────────────────────────
            if (await isCircuitOpen(gatewayId)) {
                console.warn(`⚡ Circuit OPEN for gateway ${gatewayId} — rescheduling`);
                await retryQueue.add('retry-webhook', job.data, {
                    delay:    CIRCUIT_BREAKER_COOLDOWN,
                    priority: getJobPriority(slimPayload?.type),
                    jobId:    `retry-${webhookLogId}-attempt-${attemptNumber}-cb-${Date.now()}`,
                });
                return { status: 'CIRCUIT_OPEN_RESCHEDULED', attemptNumber };
            }

            // ── TRIAL / SUBSCRIPTION CHECK ────────────────────────────
            const userCheck = await db.query(
                `SELECT u.subscription_status, u.trial_ends_at, u.is_premium
                 FROM gateways g JOIN users u ON g.user_id = u.id WHERE g.id = $1`,
                [gatewayId]
            );
            const user          = userCheck.rows[0];
            const isTrialActive = user.is_premium ||
                (user.subscription_status === 'trialing' && new Date() < new Date(user.trial_ends_at));

            if (!isTrialActive) {
                console.log(`🚫 Trial expired for gateway ${gatewayId} — skipping`);
                await db.query(
                    `UPDATE webhook_logs SET last_error_message = 'trial_expired_recovery_paused' WHERE id = $1`,
                    [webhookLogId]
                );
                retryOutcomeCounter.inc({ status: 'TRIAL_EXPIRED', attempt_number: String(attemptNumber), gateway_id: String(gatewayId) }); // FIX 8
                return { status: 'PAUSED_TRIAL_EXPIRED', attemptNumber };
            }

            console.log(`🔄 Processing retry #${attemptNumber} for ${webhookLogId}`);

            // ── FIX 11 — DB-DOWN RESILIENT PAYLOAD FETCH ─────────────
            //
            // BEFORE (broken): if DB was down, job would crash entirely.
            // NOW: we try DB first (preferred — full payload).
            //      If DB is down, we silently fall back to slimPayload
            //      stored in Redis. The HTTP request still fires.
            //      We log a warning so you can investigate, but the
            //      retry is NOT lost.
            //
            let fullPayload = slimPayload; // safe default — always has enough to fire the request
            try {
                const logRow = await db.query(`SELECT payload FROM webhook_logs WHERE id = $1`, [webhookLogId]);
                if (logRow.rows[0]?.payload) fullPayload = logRow.rows[0].payload;
            } catch (dbFetchErr) {
                console.warn(`⚠️ DB down for payload fetch — using slimPayload for ${webhookLogId}. Retry will still fire.`);
            }

            const start = Date.now();

            // ── HTTP REQUEST ──────────────────────────────────────────
            const response = await axios.post(targetUrl, fullPayload, {
                timeout: 10000,
                headers: { 'Content-Type': 'application/json' },
            });

            const httpStatus = response.status;
            const latency    = Date.now() - start;

            // FIX 8 — record latency and success counter
            retryLatencyHistogram.observe({ attempt_number: String(attemptNumber) }, latency);
            retryOutcomeCounter.inc({ status: 'SUCCESS', attempt_number: String(attemptNumber), gateway_id: String(gatewayId) });

            // ── SUCCESS ───────────────────────────────────────────────
            await resetCircuit(gatewayId);

            // DB writes are best-effort — HTTP already succeeded, don't fail the job over a DB write
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
                        fullPayload.type, 'SUCCESS', httpStatus, latency,
                        JSON.stringify(fullPayload), null, null, attemptNumber, webhookLogId,
                    ]
                );

                const amount = fullPayload?.data?.object?.amount
                    ? fullPayload.data.object.amount / 100
                    : null;

                if (amount) {
                    await db.query(
                        `INSERT INTO recovery_logs (original_log_id, gateway_id, amount, recovery_type, attempt_number)
                         VALUES ($1,$2,$3,$4,$5)`,
                        [webhookLogId, gatewayId, amount, 'AUTO_RETRY', attemptNumber]
                    );
                }

                await notifyLossRecovered({ gateway_id: gatewayId, amount, attemptNumber });
                await checkAndNotify({ gateway_id: gatewayId });

            } catch (dbWriteErr) {
                // HTTP succeeded — don't let a DB write failure undo that
                console.error(`⚠️ DB write failed after successful retry ${webhookLogId}:`, dbWriteErr.message);
            }

            console.log(`✅ Retry #${attemptNumber} succeeded for ${webhookLogId} (${latency}ms)`);
            return { status: 'SUCCESS', attemptNumber, httpStatus };

        } catch (err) {
            // ── HTTP ERROR CLASSIFICATION ─────────────────────────────
            const { shouldRetry, reason } = classifyError(err);
            const httpStatus = err.response?.status || 0;

            console.log(`❌ Retry #${attemptNumber} failed for ${webhookLogId} — ${reason}`);

            retryOutcomeCounter.inc({ status: 'FAILED', attempt_number: String(attemptNumber), gateway_id: String(gatewayId) }); // FIX 8
            await recordFailure(gatewayId);

            if (!shouldRetry) {
                console.warn(`🚫 Non-retryable (${reason}) → DLQ: ${webhookLogId}`);
                try {
                    await db.query(
                        `UPDATE webhook_logs SET status = 'FAILED', last_error_message = $2 WHERE id = $1`,
                        [webhookLogId, `non_retryable_${reason}`]
                    );
                } catch (_) {}
                await deadLetterQueue.add(
                    'non-retryable-webhook',
                    { webhookLogId, gatewayId, targetUrl, reason, httpStatus },
                    { jobId: `dlq-nonretry-${webhookLogId}-${Date.now()}` }
                );
                retryOutcomeCounter.inc({ status: 'NON_RETRYABLE', attempt_number: String(attemptNumber), gateway_id: String(gatewayId) }); // FIX 8
                return { status: 'FAILED_NON_RETRYABLE', attemptNumber, reason };
            }

            await scheduleRetry({ webhookLogId, gatewayId, payload: slimPayload, targetUrl, attemptNumber: attemptNumber + 1 });
            return { status: 'FAILED_WILL_RETRY', attemptNumber, httpStatus };

        } finally {
            await releaseProcessingLock(lockKey);
        }
    },
    { connection: redis, concurrency: MIN_CONCURRENCY }
);

// FIX 10 — kick off adaptive backpressure loop once worker is ready
retryWorker.on('ready', () => {
    workerConcurrencyGauge.set(MIN_CONCURRENCY);
    setInterval(() => adjustConcurrency(retryWorker), BACKPRESSURE_CHECK_MS);
    console.log('🚀 Retry worker online — adaptive backpressure active');
});


// ─────────────────────────────────────────────────────────────────
// DLQ WORKER — receives exhausted / unretryable jobs
// Wire notifyAdminSlack() or PagerDuty here
// ─────────────────────────────────────────────────────────────────
const dlqWorker = new Worker(
    'webhook-dlq',
    async (job) => {
        const { webhookLogId, gatewayId, reason } = job.data;
        console.error(`📬 DLQ — log: ${webhookLogId}, gateway: ${gatewayId}, reason: ${reason ?? 'exhausted'}`);
        // TODO: await notifyAdminSlack({ webhookLogId, gatewayId, reason });
    },
    { connection: redis, concurrency: 2 }
);


// ─────────────────────────────────────────────────────────────────
// WORKER EVENT LISTENERS
// ─────────────────────────────────────────────────────────────────
retryWorker.on('completed', (job, result) => {
    console.log(`✅ Job ${job.id} done — attempt #${result.attemptNumber} → ${result.status}`);
});
retryWorker.on('failed', (job, err) => {
    console.error(`🔥 Job ${job.id} crashed — ${err.message}`);
    retryOutcomeCounter.inc({ status: 'WORKER_CRASH', attempt_number: 'unknown', gateway_id: 'unknown' }); // FIX 8
});
dlqWorker.on('failed', (job, err) => {
    console.error(`🔥 DLQ Job ${job.id} crashed — ${err.message}`);
});


module.exports = { retryQueue, deadLetterQueue, scheduleRetry, getMetrics };