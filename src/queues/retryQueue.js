const { Queue, Worker, QueueEvents } = require('bullmq');
const axios = require('axios');
const redis = require('../config/redis');
const db = require('../config/db');

// ─────────────────────────────────────────────
// RETRY DELAYS — attempt number → delay in ms
// ─────────────────────────────────────────────
const RETRY_DELAYS = {
    1: 5 * 60 * 1000,          // 5 minutes
    2: 30 * 60 * 1000,         // 30 minutes
    3: 2 * 60 * 60 * 1000,     // 2 hours
    4: 24 * 60 * 60 * 1000,    // 24 hours
};

const MAX_ATTEMPTS = 4;

// ─────────────────────────────────────────────
// QUEUE — jobs are added here from controller
// ─────────────────────────────────────────────
const retryQueue = new Queue('webhook-retry', {
    connection: redis,
    defaultJobOptions: {
        removeOnComplete: 100,  // last 100 completed jobs rakhna
        removeOnFail: 200,      // last 200 failed jobs rakhna
    },
});

// ─────────────────────────────────────────────
// SCHEDULE RETRY — called from webhookController
// when charge.failed has RETRY posture
// ─────────────────────────────────────────────
const scheduleRetry = async ({ webhookLogId, gatewayId, payload, targetUrl, attemptNumber = 1 }) => {
    if (attemptNumber > MAX_ATTEMPTS) {
        // Mark as exhausted in DB — no more retries
        await db.query(
            `UPDATE webhook_logs 
             SET status = 'FAILED', 
                 last_error_message = 'retry_exhausted',
                 error_stack = error_stack || ' | Auto-retry exhausted after 4 attempts.'
             WHERE id = $1`,
            [webhookLogId]
        );
        console.log(`🔴 Retry exhausted for webhook: ${webhookLogId}`);
        return;
    }

    const delay = RETRY_DELAYS[attemptNumber];

    await retryQueue.add(
        'retry-webhook',
        {
            webhookLogId,
            gatewayId,
            payload,
            targetUrl,
            attemptNumber,
        },
        {
            delay,
            jobId: `retry-${webhookLogId}-attempt-${attemptNumber}`, // prevent duplicate jobs
        }
    );

    console.log(`⏳ Retry #${attemptNumber} scheduled for webhook ${webhookLogId} in ${delay / 1000}s`);
};


// ─────────────────────────────────────────────
// WORKER — processes retry jobs
// ─────────────────────────────────────────────
const retryWorker = new Worker(
    'webhook-retry',
    async (job) => {
        const { webhookLogId, gatewayId, payload, targetUrl, attemptNumber } = job.data;

        console.log(`🔄 Processing retry #${attemptNumber} for webhook: ${webhookLogId}`);

        const start = Date.now();
        let status = 'SUCCESS';
        let httpStatus = null;
        let errorStack = null;

        try {
            const response = await axios.post(targetUrl, payload, {
                timeout: 10000,
                headers: { 'Content-Type': 'application/json' },
            });

            httpStatus = response.status;

            // ── SUCCESS ──────────────────────────────────────────────
            // Log the successful retry as a new webhook_log entry
            await db.query(
                `INSERT INTO webhook_logs (
                    gateway_id,
                    provider_event_id,
                    event_type,
                    status,
                    http_status_code,
                    latency_ms,
                    payload,
                    error_stack,
                    last_error_message,
                    retry_count,
                    replayed_from
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
                [
                    gatewayId,
                    `${payload.id}_auto_retry_${attemptNumber}`,
                    payload.type,
                    'SUCCESS',
                    httpStatus,
                    Date.now() - start,
                    JSON.stringify(payload),
                    null,
                    null,
                    attemptNumber,
                    webhookLogId,
                ]
            );

            // Log in recovery_logs for "Loss Recovered" tracking (Feature 3)
            const amount = payload?.data?.object?.amount
                ? payload.data.object.amount / 100
                : null;

            if (amount) {
                await db.query(
                    `INSERT INTO recovery_logs (
                        original_log_id,
                        gateway_id,
                        amount,
                        recovery_type,
                        attempt_number
                    ) VALUES ($1, $2, $3, $4, $5)`,
                    [webhookLogId, gatewayId, amount, 'AUTO_RETRY', attemptNumber]
                );
            }

            console.log(`✅ Retry #${attemptNumber} succeeded for webhook: ${webhookLogId}`);

        } catch (err) {
            // ── FAILED — schedule next attempt ───────────────────────
            errorStack = err.message;
            httpStatus = err.response?.status || 0;
            status = 'FAILED';

            console.log(`❌ Retry #${attemptNumber} failed for webhook: ${webhookLogId} — ${err.message}`);

            // Schedule next attempt
            await scheduleRetry({
                webhookLogId,
                gatewayId,
                payload,
                targetUrl,
                attemptNumber: attemptNumber + 1,
            });
        }

        return { status, attemptNumber, httpStatus };
    },
    {
        connection: redis,
        concurrency: 5, // 5 retry jobs ek saath process ho sakte hain
    }
);


// ─────────────────────────────────────────────
// WORKER EVENT LISTENERS — for logging
// ─────────────────────────────────────────────
retryWorker.on('completed', (job, result) => {
    console.log(`✅ Job ${job.id} completed — attempt #${result.attemptNumber} → ${result.status}`);
});

retryWorker.on('failed', (job, err) => {
    console.error(`🔥 Job ${job.id} crashed — ${err.message}`);
});


module.exports = { retryQueue, scheduleRetry };