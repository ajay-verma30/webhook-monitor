'use strict';

const crypto = require("crypto");
const { Queue, Worker } = require("bullmq");
const axios = require("axios");
const promClient = require("prom-client");

const redis = require("../config/redis");
const db = require("../config/db");

const {
  makeRetryDecision,
  recordDeclineCodeOutcome,
  normalisePayload,
} = require("../utils/retryDecisionEngine");

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
    'SELECT provider FROM gateways WHERE id = $1',
    [gatewayId]
  );
  return res.rows[0]?.provider || 'stripe';
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
      payloadHash,
    } = job.data;

    // ⛔ Poison message detection
    if (hashPayload(payload) !== payloadHash) {
      await deadLetterQueue.add("payload-tampered", { webhookLogId });
      return;
    }

    try {
      const decision = await makeRetryDecision({
        gatewayId,
        declineCode,
        attemptNumber,
        payload,
        provider,
      });

      if (decision.action !== "RETRY") {
        await recordDeclineCodeOutcome(declineCode, "HARD_FAIL", provider);
        return;
      }

      await axios.post(targetUrl, payload, { timeout: 10000 });

      await recordDeclineCodeOutcome(declineCode, "SUCCESS", provider);

      return;

    } catch (err) {

      await recordDeclineCodeOutcome(declineCode, "FAILED", provider);

      await recordFailure(gatewayId);

      await scheduleRetry({
        webhookLogId,
        gatewayId,
        payload,
        targetUrl,
        attemptNumber: attemptNumber + 1,
        firstAttemptAt,
      });

      throw err;
    }
  },
  { connection: redis, concurrency: 10 }
);

// ─────────────────────────────────────────
module.exports = {
  retryQueue,
  deadLetterQueue,
  scheduleRetry,
};