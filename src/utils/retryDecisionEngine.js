const redis = require('../config/redis');
const db    = require('../config/db');


// ─────────────────────────────────────────────────────────────────
// FIX 12 — DECLINE CODE TAXONOMY
//
// Three buckets:
//   SOFT_FAIL       — Transient. Retry with time.
//   HARD_FAIL       — Permanent bank decision. Stop.
//   ACTION_REQUIRED — User must act first. No blind retry.
// ─────────────────────────────────────────────────────────────────

/**
 * @typedef {'SOFT_FAIL' | 'HARD_FAIL' | 'ACTION_REQUIRED'} DeclineCategory
 *
 * @typedef {Object} DeclineProfile
 * @property {DeclineCategory} category
 * @property {boolean}         retryable
 * @property {string}          reason
 * @property {number[]}        retryDelaysMs   — per-attempt delays (ms), overrides global defaults
 * @property {string|null}     userAction      — what the user should do (ACTION_REQUIRED only)
 */

/** @type {Record<string, DeclineProfile>} */
const DECLINE_CODE_PROFILES = {

    // ── SOFT_FAIL ─────────────────────────────────────────────────

    insufficient_funds: {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Card has insufficient balance. Likely recoverable after payday.',
        retryDelaysMs: [
            24 * 60 * 60 * 1000,        // 24h — after payday
             3 * 24 * 60 * 60 * 1000,   // 3d
             7 * 24 * 60 * 60 * 1000,   // 7d
        ],
        userAction: null,
    },

    card_velocity_exceeded: {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Card hit its transaction frequency limit. Resets soon.',
        retryDelaysMs: [
            1  * 60 * 60 * 1000,   // 1h
            6  * 60 * 60 * 1000,   // 6h
            24 * 60 * 60 * 1000,   // 24h
        ],
        userAction: null,
    },

    processing_error: {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Transient processing error on bank/network side.',
        retryDelaysMs: [
            5  * 60 * 1000,        // 5m
            30 * 60 * 1000,        // 30m
             2 * 60 * 60 * 1000,   // 2h
        ],
        userAction: null,
    },

    reenter_transaction: {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Temporary bank error — resubmitting should succeed.',
        retryDelaysMs: [
             2 * 60 * 1000,        // 2m
            15 * 60 * 1000,        // 15m
            60 * 60 * 1000,        // 1h
        ],
        userAction: null,
    },

    try_again_later: {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Bank temporarily unavailable.',
        retryDelaysMs: [
            30 * 60 * 1000,        // 30m
             2 * 60 * 60 * 1000,   // 2h
             6 * 60 * 60 * 1000,   // 6h
        ],
        userAction: null,
    },

    // ── HARD_FAIL ─────────────────────────────────────────────────

    do_not_honor: {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Generic hard decline from bank. Opaque — requires DNH pattern analysis before any retry decision.',
        retryDelaysMs: [],
        userAction:    'Contact your bank or use a different payment method.',
    },

    card_not_supported: {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Card type does not support this transaction.',
        retryDelaysMs: [],
        userAction:    'Use a different card (e.g. credit instead of debit).',
    },

    currency_not_supported: {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Card does not support the billing currency.',
        retryDelaysMs: [],
        userAction:    'Use a card that supports this currency.',
    },

    invalid_account: {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Account no longer exists at the issuing bank.',
        retryDelaysMs: [],
        userAction:    'Update payment method — this card account is closed.',
    },

    stolen_card: {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Card reported stolen.',
        retryDelaysMs: [],
        userAction:    'Do not retry. Flag account for review.',
    },

    lost_card: {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Card reported lost.',
        retryDelaysMs: [],
        userAction:    'Do not retry. Customer needs a replacement card.',
    },

    pickup_card: {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Bank instructed to physically seize this card.',
        retryDelaysMs: [],
        userAction:    'Do not retry. Flag account.',
    },

    blocked_by_stripe: {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Stripe blocked this card for fraud/risk reasons.',
        retryDelaysMs: [],
        userAction:    'Contact Stripe support.',
    },

    // ── ACTION_REQUIRED ───────────────────────────────────────────

    authentication_required: {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        '3DS/SCA required. Issuer wants user to authenticate.',
        retryDelaysMs: [],
        userAction:    'Trigger 3DS authentication flow — do not blindly retry.',
    },

    card_declined: {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'Generic decline — needs user clarification.',
        retryDelaysMs: [],
        userAction:    'Ask customer to contact their bank or try another card.',
    },

    expired_card: {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'Card is past its expiry date.',
        retryDelaysMs: [],
        userAction:    'Ask customer to update their card details.',
    },

    incorrect_cvc: {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'CVC verification failed.',
        retryDelaysMs: [],
        userAction:    'Ask customer to re-enter CVC.',
    },

    incorrect_number: {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'Card number is invalid.',
        retryDelaysMs: [],
        userAction:    'Ask customer to re-enter card details.',
    },

    incorrect_zip: {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'Billing ZIP mismatch (AVS check failed).',
        retryDelaysMs: [],
        userAction:    'Ask customer to verify billing address ZIP.',
    },
};

/**
 * Resolve a Stripe decline code to its full profile.
 * Falls back to a safe SOFT_FAIL for unknown codes.
 *
 * @param {string|undefined} code
 * @returns {DeclineProfile}
 */
const resolveDeclineProfile = (code) => {
    if (!code) return _unknownProfile('no_decline_code');
    return DECLINE_CODE_PROFILES[code] ?? _unknownProfile(code);
};

const _unknownProfile = (code) => ({
    category:      'SOFT_FAIL',
    retryable:     true,
    reason:        `Unrecognised decline code: ${code} — defaulting to soft fail`,
    retryDelaysMs: [5 * 60 * 1000, 30 * 60 * 1000, 2 * 60 * 60 * 1000, 24 * 60 * 60 * 1000],
    userAction:    null,
});


// ─────────────────────────────────────────────────────────────────
// FIX 13 — SMART RETRY DELAYS
//
// Each profile carries its own retryDelaysMs[].
// Falls back to global defaults if the profile has no custom schedule.
// ─────────────────────────────────────────────────────────────────
const GLOBAL_RETRY_DELAYS_MS = {
    1: 5  * 60 * 1000,
    2: 30 * 60 * 1000,
    3: 2  * 60 * 60 * 1000,
    4: 24 * 60 * 60 * 1000,
};

/**
 * @param {number}         attemptNumber  1-based
 * @param {DeclineProfile} profile
 * @returns {number} base delay in ms (before jitter)
 */
const getSmartDelay = (attemptNumber, profile) => {
    const custom = profile?.retryDelaysMs;
    if (custom?.length >= attemptNumber) return custom[attemptNumber - 1];
    return GLOBAL_RETRY_DELAYS_MS[attemptNumber] ?? GLOBAL_RETRY_DELAYS_MS[4];
};

/**
 * Add ±20% jitter to prevent retry storms.
 * @param {number} baseDelay ms
 * @returns {number} ms
 */
const withJitter = (baseDelay) => {
    const jitter = baseDelay * 0.2 * (Math.random() * 2 - 1);
    return Math.max(1000, Math.round(baseDelay + jitter));
};


// ─────────────────────────────────────────────────────────────────
// FIX 14 — RCA PATTERN DETECTION
//
// "do_not_honor" is a black box. We detect whether it's a one-off
// or a systematic pattern by tracking hits per
// (gatewayId, country, cardBrand, issuer) in Redis.
//
// ≥ DNH_PATTERN_THRESHOLD hits in DNH_WINDOW_SECONDS
//   → upgrade do_not_honor to HARD_FAIL for that pattern.
// ─────────────────────────────────────────────────────────────────
const DNH_PATTERN_THRESHOLD = 3;
const DNH_WINDOW_SECONDS    = 600; // 10 minutes

const _dnhKey = ({ gatewayId, cardCountry, cardBrand, issuer }) =>
    `dnh_pattern:${gatewayId}:${cardCountry}:${cardBrand}:${issuer}`;

/**
 * Record a do_not_honor hit and detect if it's becoming a pattern.
 *
 * @returns {Promise<{ isPattern: boolean, count: number }>}
 */
const recordDNHPattern = async ({ gatewayId, cardCountry, cardBrand, issuer }) => {
    try {
        const key   = _dnhKey({ gatewayId, cardCountry, cardBrand, issuer });
        const count = await redis.incr(key);
        await redis.expire(key, DNH_WINDOW_SECONDS);

        if (count >= DNH_PATTERN_THRESHOLD) {
            console.warn(
                `⚠️ DNH pattern detected: gateway=${gatewayId}, ` +
                `country=${cardCountry}, brand=${cardBrand}, issuer=${issuer} — count=${count}`
            );
            // Persist for dashboards/offline analysis
            try {
                await db.query(
                    `INSERT INTO decline_patterns
                         (gateway_id, card_country, card_brand, issuer, pattern_count, detected_at)
                     VALUES ($1,$2,$3,$4,$5,NOW())
                     ON CONFLICT (gateway_id, card_country, card_brand, issuer)
                     DO UPDATE SET pattern_count = $4, detected_at = NOW()`,
                    [gatewayId, cardCountry, cardBrand, issuer, count]
                );
            } catch (_) {}
            return { isPattern: true, count };
        }
        return { isPattern: false, count };
    } catch (_) {
        return { isPattern: false, count: 0 };
    }
};

/**
 * Check if a DNH pattern already exists (without incrementing).
 * Used before deciding whether to retry a do_not_honor failure.
 *
 * @returns {Promise<boolean>}
 */
const isDNHPatternKnown = async ({ gatewayId, cardCountry, cardBrand, issuer }) => {
    try {
        const count = parseInt(await redis.get(_dnhKey({ gatewayId, cardCountry, cardBrand, issuer })) ?? '0', 10);
        return count >= DNH_PATTERN_THRESHOLD;
    } catch (_) {
        return false; // fail open — don't block on Redis being down
    }
};


// ─────────────────────────────────────────────────────────────────
// FIX 15 — SUCCESS PROBABILITY SCORING
//
// Before scheduling a retry, score it 0–1.
// If score < threshold → skip, send to DLQ.
// Gets smarter over time as decline_code_stats fills up.
// ─────────────────────────────────────────────────────────────────
const SUCCESS_PROBABILITY_THRESHOLD = 0.10;
const MIN_HISTORICAL_SAMPLES        = 20;

/** @type {Record<DeclineCategory, number[]>} — [attempt1, attempt2, attempt3, attempt4] */
const BASE_SUCCESS_PROBABILITY = {
    SOFT_FAIL:       [0.60, 0.45, 0.30, 0.15],
    HARD_FAIL:       [0.02, 0.01, 0.01, 0.00],
    ACTION_REQUIRED: [0.05, 0.02, 0.01, 0.00],
};

/**
 * @param {Object}         p
 * @param {DeclineProfile} p.profile
 * @param {number}         p.attemptNumber   1-based
 * @param {boolean}        p.isDNHPattern
 * @param {number|null}    p.historicalRate  null = insufficient data
 * @returns {number} 0.0 – 1.0
 */
const computeSuccessProbability = ({ profile, attemptNumber, isDNHPattern, historicalRate }) => {
    const idx      = Math.min(attemptNumber - 1, 3);
    const base     = BASE_SUCCESS_PROBABILITY[profile.category]?.[idx] ?? 0.05;
    const adjusted = isDNHPattern ? base * 0.5 : base;

    // Weight real data 60% once we have enough samples
    if (historicalRate !== null && historicalRate !== undefined) {
        return 0.60 * historicalRate + 0.40 * adjusted;
    }
    return adjusted;
};

/**
 * Fetch historical success rate for a decline code from the last 30 days.
 * Returns null if < MIN_HISTORICAL_SAMPLES rows exist.
 *
 * @param {string} declineCode
 * @returns {Promise<number|null>}
 */
const fetchHistoricalSuccessRate = async (declineCode) => {
    try {
        const res = await db.query(
            `SELECT
                COUNT(*) FILTER (WHERE outcome = 'SUCCESS') AS successes,
                COUNT(*)                                     AS total
             FROM decline_code_stats
             WHERE decline_code = $1
               AND recorded_at  > NOW() - INTERVAL '30 days'`,
            [declineCode]
        );
        const { successes, total } = res.rows[0] ?? {};
        if (!total || parseInt(total, 10) < MIN_HISTORICAL_SAMPLES) return null;
        return parseInt(successes, 10) / parseInt(total, 10);
    } catch (_) {
        return null;
    }
};

/**
 * Record a retry outcome so future probability calculations improve.
 *
 * @param {string}                          declineCode
 * @param {'SUCCESS'|'FAILED'|'HARD_FAIL'}  outcome
 */
const recordDeclineCodeOutcome = async (declineCode, outcome) => {
    try {
        await db.query(
            `INSERT INTO decline_code_stats (decline_code, outcome, recorded_at)
             VALUES ($1, $2, NOW())`,
            [declineCode, outcome]
        );
    } catch (_) {}
};


// ─────────────────────────────────────────────────────────────────
// FIX 16 — CROSS-EVENT JOURNEY INTELLIGENCE
//
// Tracks the full Stripe payment lifecycle per customer in Redis:
//   payment_intent.payment_failed
//     → charge.failed (decline_code lives here)
//       → invoice.payment_failed
//         → subscription → past_due → deleted
//
// Gives the retry worker full context:
//   • Is there a subscription at risk?
//   • How many times has this invoice failed?
//   • Is 3DS required?
//   • What is the urgency level?
// ─────────────────────────────────────────────────────────────────

/**
 * @typedef {Object} JourneyContext
 * @property {boolean}                        hasActiveSubscription
 * @property {boolean}                        isSubscriptionPastDue
 * @property {boolean}                        isSubscriptionAtRisk   — 3+ failed attempts
 * @property {boolean}                        requires3DS
 * @property {string}                         declineCode
 * @property {string}                         cardCountry
 * @property {string}                         cardBrand
 * @property {string}                         issuer
 * @property {number}                         journeyAttemptCount
 * @property {'LOW'|'MEDIUM'|'HIGH'|'CRITICAL'} urgency
 */

const JOURNEY_TTL_SECONDS = 30 * 24 * 60 * 60; // 30 days — covers longest retry window

/**
 * Build full journey context from a Stripe event payload
 * combined with any prior state stored in Redis.
 *
 * @param {Object}        p
 * @param {string|number} p.gatewayId
 * @param {Object}        p.payload     Full Stripe event object
 * @returns {Promise<JourneyContext>}
 */
const buildJourneyContext = async ({ gatewayId, payload }) => {
    const customerId  = payload?.data?.object?.customer ?? 'unknown';
    const journeyKey  = `journey:${gatewayId}:${customerId}`;

    let priorState = {};
    try {
        const raw = await redis.get(journeyKey);
        if (raw) priorState = JSON.parse(raw);
    } catch (_) {}

    const chargeObj  = payload?.data?.object ?? {};
    const piObj      = chargeObj?.payment_intent ?? {};
    const outcomeObj = chargeObj?.outcome ?? {};
    const cardDetail = chargeObj?.payment_method_details?.card ?? {};

    const declineCode = outcomeObj?.network_decline_code ?? outcomeObj?.reason ?? 'unknown';
    const cardCountry = cardDetail?.country ?? 'unknown';
    const cardBrand   = cardDetail?.brand   ?? 'unknown';
    const issuer      = cardDetail?.issuer  ?? 'unknown';

    const requires3DS = !!(
        piObj?.next_action?.type === 'use_stripe_sdk' ||
        piObj?.next_action?.type === 'redirect_to_url'
    );

    const journeyAttemptCount = (priorState.attemptCount ?? 0) + 1;
    const isSubscription      = !!chargeObj?.invoice;
    const isPastDue           = priorState.subscriptionStatus === 'past_due';
    const isHighValue         = (chargeObj?.amount ?? 0) > 10_000; // > $100

    let urgency = 'LOW';
    if (journeyAttemptCount >= 3 && isSubscription) urgency = 'CRITICAL';
    else if (isPastDue || isHighValue)               urgency = 'HIGH';
    else if (journeyAttemptCount >= 2)               urgency = 'MEDIUM';

    // Persist updated state
    try {
        await redis.set(
            journeyKey,
            JSON.stringify({
                ...priorState,
                declineCode,
                cardCountry,
                cardBrand,
                issuer,
                requires3DS,
                attemptCount:       journeyAttemptCount,
                lastFailedAt:       new Date().toISOString(),
                subscriptionStatus: priorState.subscriptionStatus ?? 'unknown',
            }),
            'EX', JOURNEY_TTL_SECONDS
        );
    } catch (_) {}

    return {
        hasActiveSubscription: isSubscription,
        isSubscriptionPastDue: isPastDue,
        isSubscriptionAtRisk:  journeyAttemptCount >= 3 && isSubscription,
        requires3DS,
        declineCode,
        cardCountry,
        cardBrand,
        issuer,
        journeyAttemptCount,
        urgency,
    };
};

/**
 * Update subscription status in the journey store.
 * Call this from webhookController on:
 *   customer.subscription.updated / customer.subscription.deleted
 *
 * @param {Object}        p
 * @param {string|number} p.gatewayId
 * @param {string}        p.customerId   Stripe customer ID
 * @param {string}        p.status       Stripe subscription status
 */
const updateJourneySubscriptionStatus = async ({ gatewayId, customerId, status }) => {
    const journeyKey = `journey:${gatewayId}:${customerId}`;
    try {
        const raw   = await redis.get(journeyKey);
        const state = raw ? JSON.parse(raw) : {};
        state.subscriptionStatus = status;
        await redis.set(journeyKey, JSON.stringify(state), 'EX', JOURNEY_TTL_SECONDS);
    } catch (_) {}
};


// ─────────────────────────────────────────────────────────────────
// MAIN EXPORT — makeRetryDecision
//
// The single function the infra layer calls.
// Returns a plain decision object — no side effects on queues.
// ─────────────────────────────────────────────────────────────────

/**
 * @typedef {Object} RetryDecision
 * @property {'RETRY'|'HARD_STOP'|'ACTION_REQUIRED'|'LOW_PROBABILITY_STOP'} action
 * @property {DeclineProfile}  profile           — resolved decline profile
 * @property {JourneyContext}  journeyContext     — full journey state
 * @property {number}          delayMs           — how long to wait before next attempt (with jitter)
 * @property {number}          priority          — BullMQ job priority (higher = sooner)
 * @property {number}          successProbability — 0–1 score
 * @property {string|null}     userAction        — message to show the customer (if any)
 * @property {string}          reason            — human-readable explanation of the decision
 */

const EVENT_PRIORITY = {
    'charge.failed':                 10,
    'payment_intent.payment_failed': 10,
    'charge.dispute.created':         7,
    'customer.subscription.deleted':  5,
};
const URGENCY_PRIORITY_BONUS = { CRITICAL: 5, HIGH: 3, MEDIUM: 1, LOW: 0 };

/**
 * Evaluate a failure and return a complete retry decision.
 *
 * @param {Object}        p
 * @param {string|number} p.gatewayId
 * @param {string}        p.declineCode       from Stripe charge.outcome
 * @param {number}        p.attemptNumber     1-based
 * @param {Object}        p.payload           full Stripe event payload
 * @returns {Promise<RetryDecision>}
 */
const makeRetryDecision = async ({ gatewayId, declineCode, attemptNumber, payload }) => {

    // ── 1. Resolve decline profile ────────────────────────────────
    let profile = resolveDeclineProfile(declineCode);

    // ── 2. Build journey context (FIX 16) ─────────────────────────
    const journeyContext = await buildJourneyContext({ gatewayId, payload });

    // ── 3. 3DS override: always ACTION_REQUIRED (FIX 16) ──────────
    if (journeyContext.requires3DS) {
        profile = {
            ...profile,
            category:   'ACTION_REQUIRED',
            retryable:  false,
            userAction: 'Trigger 3DS authentication flow — do not blindly retry.',
        };
    }

    // ── 4. DNH pattern detection (FIX 14) ─────────────────────────
    let isDNHPattern = false;
    if (declineCode === 'do_not_honor') {
        const { isPattern } = await recordDNHPattern({
            gatewayId,
            cardCountry: journeyContext.cardCountry,
            cardBrand:   journeyContext.cardBrand,
            issuer:      journeyContext.issuer,
        });
        isDNHPattern = isPattern;
        if (isPattern) {
            profile = { ...profile, category: 'HARD_FAIL', retryable: false };
        }
    }

    // ── 5. Success probability (FIX 15) ───────────────────────────
    const historicalRate     = await fetchHistoricalSuccessRate(declineCode);
    const successProbability = computeSuccessProbability({
        profile,
        attemptNumber,
        isDNHPattern,
        historicalRate,
    });

    // ── 6. Final decision ─────────────────────────────────────────
    const basePriority = EVENT_PRIORITY[payload?.type] ?? 1;
    const bonus        = URGENCY_PRIORITY_BONUS[journeyContext.urgency] ?? 0;
    const priority     = Math.min(basePriority + bonus, 20);

    if (successProbability < SUCCESS_PROBABILITY_THRESHOLD) {
        return {
            action:            'LOW_PROBABILITY_STOP',
            profile,
            journeyContext,
            delayMs:           0,
            priority,
            successProbability,
            userAction:        profile.userAction,
            reason:            `Success probability too low (${(successProbability * 100).toFixed(1)}%) — not worth retrying`,
        };
    }

    if (!profile.retryable) {
        return {
            action:            profile.category === 'ACTION_REQUIRED' ? 'ACTION_REQUIRED' : 'HARD_STOP',
            profile,
            journeyContext,
            delayMs:           0,
            priority,
            successProbability,
            userAction:        profile.userAction,
            reason:            profile.reason,
        };
    }

    const baseDelay = getSmartDelay(attemptNumber, profile);
    const delayMs   = withJitter(baseDelay);

    return {
        action:            'RETRY',
        profile,
        journeyContext,
        delayMs,
        priority,
        successProbability,
        userAction:        null,
        reason:            profile.reason,
    };
};


module.exports = {
    makeRetryDecision,                  
    buildJourneyContext,
    updateJourneySubscriptionStatus,
    recordDeclineCodeOutcome,
    DECLINE_CODE_PROFILES,
    resolveDeclineProfile,
    computeSuccessProbability,
    SUCCESS_PROBABILITY_THRESHOLD,
};