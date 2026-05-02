'use strict';

const { Queue, Worker } = require("bullmq");
const axios = require("axios");
const promClient = require("prom-client");
const crypto = require('crypto');

const redis = require("../config/redis");
const db = require("../config/db");

const {
  makeRetryDecision,
  recordDeclineCodeOutcome,
  normalisePayload,
} = require("../utils/retryDecisionEngine");


const generateHash = (data) => {
  return crypto
    .createHash('sha256')
    .update(JSON.stringify(data))
    .digest('hex');
};
// ─────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────
const MAX_ATTEMPTS = 4;
const MAX_RETRY_WINDOW_MS = 24 * 60 * 60 * 1000; // 24h
const CIRCUIT_BREAKER_THRESHOLD = 10;
const CIRCUIT_BREAKER_TTL = 60; // seconds
const RATE_LIMIT_PER_MIN = 60;

// ─────────────────────────────────────────
// METRICS
// ─────────────────────────────────────────
promClient.collectDefaultMetrics({ prefix: "webhook_retry_" });

// ─────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────

const hashPayload = (payload) =>
  crypto.createHash("sha256").update(JSON.stringify(payload)).digest("hex");

const safeJsonParse = (str) => {
  try { return JSON.parse(str); } catch { return null; }
};

const getGatewayProvider = async (gatewayId) => {
  const res = await db.query(
    'SELECT provider_type FROM gateways WHERE id = $1',
    [gatewayId]
  );
  return res.rows[0]?.provider_type?.toLowerCase() || 'stripe';
};

const isDuplicate = async (idempotencyKey) => {
  const exists = await redis.get(idempotencyKey);
  if (exists) return true;
  await redis.set(idempotencyKey, "1", "EX", 60 * 60);
  return false;
};

const isCircuitOpen = async (gatewayId) => {
  const key = `circuit:${gatewayId}`;
  const val = await redis.get(key);
  return parseInt(val || "0") >= CIRCUIT_BREAKER_THRESHOLD;
};

const recordFailure = async (gatewayId) => {
  const key = `circuit:${gatewayId}`;
  const count = await redis.incr(key);
  if (count === 1) await redis.expire(key, CIRCUIT_BREAKER_TTL);
};

const checkRateLimit = async (gatewayId) => {
  const key = `rate:${gatewayId}:${Math.floor(Date.now() / 60000)}`;
  const count = await redis.incr(key);
  if (count === 1) await redis.expire(key, 60);
  return count <= RATE_LIMIT_PER_MIN;
};

// ─────────────────────────────────────────
// QUEUES
// ─────────────────────────────────────────
const retryQueue = new Queue("webhook-retry", { connection: redis });
const deadLetterQueue = new Queue("webhook-dlq", { connection: redis });

// ─────────────────────────────────────────
// SCHEDULE RETRY
// ─────────────────────────────────────────
const scheduleRetry = async ({
  webhookLogId,
  gatewayId,
  payload,
  targetUrl,
  attemptNumber = 1,
  firstAttemptAt = Date.now(),
}) => {

  const provider = await getGatewayProvider(gatewayId);
  const norm = normalisePayload(payload, provider);

  // ⛔ MAX ATTEMPTS
  if (attemptNumber > MAX_ATTEMPTS) {
    await deadLetterQueue.add("exhausted", { webhookLogId });
    return;
  }

  // ⛔ MAX TIME WINDOW
  if (Date.now() - firstAttemptAt > MAX_RETRY_WINDOW_MS) {
    await deadLetterQueue.add("expired", { webhookLogId });
    return;
  }

  // ⛔ DUPLICATE
  const idempotencyKey = `retry:${webhookLogId}:${attemptNumber}`;
  if (await isDuplicate(idempotencyKey)) return;

  // ⛔ CIRCUIT BREAKER
  if (await isCircuitOpen(gatewayId)) {
    await deadLetterQueue.add("circuit-open", { webhookLogId });
    return;
  }

  // ⛔ RATE LIMIT
  if (!(await checkRateLimit(gatewayId))) {
    await deadLetterQueue.add("rate-limited", { webhookLogId });
    return;
  }

  const decision = await makeRetryDecision({
    gatewayId,
    declineCode: norm.declineCode,
    attemptNumber,
    payload,
    provider,
  });

  if (decision.action !== "RETRY") {
    await deadLetterQueue.add("engine-stop", {
      webhookLogId,
      reason: decision.reason,
    });
    return;
  }

  await retryQueue.add(
    "retry-webhook",
    {
      webhookLogId,
      gatewayId,
      provider,
      payload,
      targetUrl,
      attemptNumber,
      declineCode: norm.declineCode,
      firstAttemptAt,
      payloadHash: hashPayload(payload),
    },
    {
      delay: decision.delayMs,
      priority: decision.priority,
      jobId: `retry-${webhookLogId}-${attemptNumber}`,
    }
  );
  console.log(`✅ Retry job queued: webhookLogId=${webhookLogId} attempt=${attemptNumber} delay=${decision.delayMs}ms`);
};

// ─────────────────────────────────────────
// WORKER
// ─────────────────────────────────────────
const retryWorker = new Worker(
  "webhook-retry",
  async (job) => {
    const {
      webhookLogId,
      gatewayId,
      provider,
      payload,
      targetUrl,
      attemptNumber,
      declineCode,
      firstAttemptAt,
      payloadHash, // The hash we stored when we first queued the job
    } = job.data;

    console.log(`🔄 [Attempt #${attemptNumber}] Processing ID: ${webhookLogId}`);

    // ⛔ 1. Integrity Check (Poison Message Detection)
    // We re-hash the current payload and compare it to the original hash
    const currentHash = generateHash(payload);
    if (payloadHash && currentHash !== payloadHash) {
      console.error(`🚨 SECURITY: Payload tampered for ${webhookLogId}. Dropping job.`);
      return; 
    }

    try {
      // 🚀 2. Delivery to Target URL
      const start = Date.now();
      await axios.post(targetUrl, payload, { 
        timeout: 10000,
        headers: { 
          'Content-Type': 'application/json',
          'X-HookPulse-Retry-Attempt': attemptNumber 
        } 
      });
      const latency = Date.now() - start;

      // 💰 3. Extraction (Converting Cents to Dollars)
      const rawAmount = payload?.data?.object?.amount || 0;
      const formattedAmount = rawAmount / 100; 

      // 📝 4. Update Database: Log the Recovery
      // This makes the "Recovered Revenue" number go up!
      await db.query(
        `INSERT INTO recovery_logs 
         (original_log_id, gateway_id, amount, recovery_type, attempt_number)
         VALUES ($1, $2, $3, 'AUTO_RETRY', $4)`,
        [webhookLogId, gatewayId, formattedAmount, attemptNumber]
      );

      // 📝 5. Update Database: Mark original log as SUCCESS
      await db.query(
        `UPDATE webhook_logs 
         SET status = 'SUCCESS', 
             http_status_code = 200, 
             latency_ms = $1,
             retry_count = $2
         WHERE id = $3`,
        [latency, attemptNumber, webhookLogId]
      );

      console.log(`✅ SUCCESS: $${formattedAmount} recovered for ${webhookLogId}`);
      return { status: 'SUCCESS', amount: formattedAmount };

    } catch (err) {
      const statusCode = err.response?.status || 0;
      console.error(`❌ Attempt #${attemptNumber} failed: ${err.message}`);

      // 📉 6. Schedule Next Retry (If under max attempts)
      // Note: Ensure scheduleRetry is imported or defined in this file
      if (typeof scheduleRetry === 'function') {
        await scheduleRetry({
          webhookLogId,
          gatewayId,
          payload,
          targetUrl,
          attemptNumber: attemptNumber + 1,
          declineCode,
          firstAttemptAt,
          payloadHash: currentHash
        });
      }

      throw err; // BullMQ will handle the job failure state
    }
  },
  { 
    connection: redis, // Ensure 'redis' is initialized in this file
    concurrency: 10 
  }
);

// ─────────────────────────────────────────
module.exports = {
  retryQueue,
  deadLetterQueue,
  scheduleRetry,
};