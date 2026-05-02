'use strict';

const redis = require('../config/redis');
const db    = require('../config/db');

// ─────────────────────────────────────────────────────────────────
// DECLINE CODE TAXONOMY
//
// Three buckets:
//   SOFT_FAIL       — Transient. Retry after a delay.
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
 * @property {number[]}        retryDelaysMs   — per-attempt delays (ms); overrides global defaults
 * @property {string|null}     userAction      — message to show the customer (ACTION_REQUIRED only)
 */

// ─── Stripe ───────────────────────────────────────────────────────────────────

/** @type {Record<string, DeclineProfile>} */
const DECLINE_CODE_PROFILES = {

    // ── SOFT_FAIL ─────────────────────────────────────────────────────────────

    insufficient_funds: {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Card has insufficient balance. Likely recoverable after payday.',
        retryDelaysMs: [
            24 * 60 * 60 * 1000,       // 24h
             3 * 24 * 60 * 60 * 1000,  // 3d
             7 * 24 * 60 * 60 * 1000,  // 7d
        ],
        userAction: null,
    },
    card_velocity_exceeded: {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Card hit its transaction frequency limit. Resets soon.',
        retryDelaysMs: [1 * 60 * 60 * 1000, 6 * 60 * 60 * 1000, 24 * 60 * 60 * 1000],
        userAction:    null,
    },
    processing_error: {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Transient processing error on bank/network side.',
        retryDelaysMs: [5 * 60 * 1000, 30 * 60 * 1000, 2 * 60 * 60 * 1000],
        userAction:    null,
    },
    reenter_transaction: {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Temporary bank error — resubmitting should succeed.',
        retryDelaysMs: [2 * 60 * 1000, 15 * 60 * 1000, 60 * 60 * 1000],
        userAction:    null,
    },
    try_again_later: {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Bank temporarily unavailable.',
        retryDelaysMs: [30 * 60 * 1000, 2 * 60 * 60 * 1000, 6 * 60 * 60 * 1000],
        userAction:    null,
    },

    // ── HARD_FAIL ─────────────────────────────────────────────────────────────

    do_not_honor: {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Generic hard decline from bank. Requires DNH pattern analysis before any retry decision.',
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

    // ── ACTION_REQUIRED ───────────────────────────────────────────────────────

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

// ─── Adyen ────────────────────────────────────────────────────────────────────
// Source: https://docs.adyen.com/development-resources/refusal-reasons

/** @type {Record<string, DeclineProfile>} */
const ADYEN_DECLINE_CODE_PROFILES = {

    // ── SOFT_FAIL ─────────────────────────────────────────────────────────────

    '2': {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Adyen: Issuer temporarily unavailable or unreachable.',
        retryDelaysMs: [5 * 60 * 1000, 30 * 60 * 1000, 2 * 60 * 60 * 1000],
        userAction:    null,
    },
    '6': {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Adyen: Transient processing error on the acquirer/issuer side.',
        retryDelaysMs: [5 * 60 * 1000, 30 * 60 * 1000, 2 * 60 * 60 * 1000],
        userAction:    null,
    },
    '17': {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Adyen: Insufficient funds — likely recoverable after payday.',
        retryDelaysMs: [
            24 * 60 * 60 * 1000,
             3 * 24 * 60 * 60 * 1000,
             7 * 24 * 60 * 60 * 1000,
        ],
        userAction: null,
    },
    '26': {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Adyen: Card velocity limit exceeded — resets after the card limit window.',
        retryDelaysMs: [1 * 60 * 60 * 1000, 6 * 60 * 60 * 1000, 24 * 60 * 60 * 1000],
        userAction:    null,
    },
    'ADYEN_RETRY_LATER': {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'Adyen: Soft decline — issuer asked to retry later.',
        retryDelaysMs: [10 * 60 * 1000, 60 * 60 * 1000, 6 * 60 * 60 * 1000],
        userAction:    null,
    },

    // ── HARD_FAIL ─────────────────────────────────────────────────────────────

    '8': {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Adyen: Card is blocked by the issuing bank.',
        retryDelaysMs: [],
        userAction:    'Contact your bank or use a different payment method.',
    },
    '12': {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Adyen: Card number is invalid.',
        retryDelaysMs: [],
        userAction:    'Re-enter card details — card number is incorrect.',
    },
    '15': {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Adyen: Transaction flagged as fraud by acquirer.',
        retryDelaysMs: [],
        userAction:    'Do not retry. Flag account for review.',
    },
    '20': {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Adyen: Transaction declined for fraud reasons.',
        retryDelaysMs: [],
        userAction:    'Do not retry. Flag account for review.',
    },
    '23': {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Adyen: Card type not permitted for this transaction.',
        retryDelaysMs: [],
        userAction:    'Use a different card type (e.g. credit instead of debit).',
    },
    // Adyen reason 27 — generic do-not-honor equivalent; DNH pattern detection applies
    '27': {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'Adyen: Generic hard decline from issuer (do-not-honor equivalent).',
        retryDelaysMs: [],
        userAction:    'Contact your bank or use a different payment method.',
    },

    // ── ACTION_REQUIRED ───────────────────────────────────────────────────────

    '14': {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'Adyen: Card PAN is invalid or expired.',
        retryDelaysMs: [],
        userAction:    'Ask customer to update their card details.',
    },
    '22': {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'Adyen: Bank declined — cardholder must contact their bank.',
        retryDelaysMs: [],
        userAction:    'Ask customer to contact their bank before retrying.',
    },
    '24': {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'Adyen: CVC check failed.',
        retryDelaysMs: [],
        userAction:    'Ask customer to re-enter their CVC.',
    },
    'ADYEN_3DS_REQUIRED': {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'Adyen: 3DS/SCA authentication required by the issuer.',
        retryDelaysMs: [],
        userAction:    'Trigger 3DS authentication flow — do not blindly retry.',
    },
};

// ─── PayPal ───────────────────────────────────────────────────────────────────
// Source: https://developer.paypal.com/docs/api/payments/v1/#definition-processor_response

/** @type {Record<string, DeclineProfile>} */
const PAYPAL_DECLINE_CODE_PROFILES = {

    // ── SOFT_FAIL ─────────────────────────────────────────────────────────────

    'PAYPAL_INTERNAL_ERROR': {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'PayPal: Internal processing error — transient, safe to retry.',
        retryDelaysMs: [5 * 60 * 1000, 30 * 60 * 1000, 2 * 60 * 60 * 1000],
        userAction:    null,
    },
    'PAYPAL_GATEWAY_TIMEOUT': {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'PayPal: Gateway timeout — upstream issuer did not respond in time.',
        retryDelaysMs: [2 * 60 * 1000, 15 * 60 * 1000, 60 * 60 * 1000],
        userAction:    null,
    },
    'INSUFFICIENT_FUNDS': {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'PayPal: Insufficient funds — likely recoverable after payday.',
        retryDelaysMs: [
            24 * 60 * 60 * 1000,
             3 * 24 * 60 * 60 * 1000,
             7 * 24 * 60 * 60 * 1000,
        ],
        userAction: null,
    },
    'PAYPAL_LIMIT_EXCEEDED': {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'PayPal: Transaction limit exceeded for this card — resets after limit window.',
        retryDelaysMs: [1 * 60 * 60 * 1000, 6 * 60 * 60 * 1000, 24 * 60 * 60 * 1000],
        userAction:    null,
    },
    'TRY_AGAIN_LATER': {
        category:      'SOFT_FAIL',
        retryable:     true,
        reason:        'PayPal: Issuer temporarily unavailable — retry later.',
        retryDelaysMs: [30 * 60 * 1000, 2 * 60 * 60 * 1000, 6 * 60 * 60 * 1000],
        userAction:    null,
    },

    // ── HARD_FAIL ─────────────────────────────────────────────────────────────

    // PayPal do-not-honor equivalent; DNH pattern detection applies
    'DO_NOT_HONOR': {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'PayPal: Generic hard decline from issuer — requires DNH pattern analysis.',
        retryDelaysMs: [],
        userAction:    'Contact your bank or use a different payment method.',
    },
    'CARD_CLOSED': {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'PayPal: Card account is closed at the issuing bank.',
        retryDelaysMs: [],
        userAction:    'Update payment method — this card account is closed.',
    },
    'PAYPAL_RISK_DECLINE': {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'PayPal: Transaction blocked by PayPal risk/fraud systems.',
        retryDelaysMs: [],
        userAction:    'Do not retry. Flag account for review.',
    },
    'CURRENCY_NOT_SUPPORTED': {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'PayPal: Card does not support the billing currency.',
        retryDelaysMs: [],
        userAction:    'Use a card that supports this currency.',
    },
    'CARD_TYPE_NOT_SUPPORTED': {
        category:      'HARD_FAIL',
        retryable:     false,
        reason:        'PayPal: Card type not supported for this transaction.',
        retryDelaysMs: [],
        userAction:    'Use a different card type (e.g. credit instead of debit).',
    },

    // ── ACTION_REQUIRED ───────────────────────────────────────────────────────

    'PAYPAL_3DS_REQUIRED': {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'PayPal: 3DS/SCA authentication required.',
        retryDelaysMs: [],
        userAction:    'Trigger 3DS authentication flow — do not blindly retry.',
    },
    'EXPIRED_CARD': {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'PayPal: Card is past its expiry date.',
        retryDelaysMs: [],
        userAction:    'Ask customer to update their card details.',
    },
    'INVALID_CVV2': {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'PayPal: CVV2/CVC verification failed.',
        retryDelaysMs: [],
        userAction:    'Ask customer to re-enter their CVV.',
    },
    'INVALID_ACCOUNT': {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'PayPal: Card number or account is invalid.',
        retryDelaysMs: [],
        userAction:    'Ask customer to re-enter card details.',
    },
    'AVS_MISMATCH': {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'PayPal: Billing address (AVS) check failed.',
        retryDelaysMs: [],
        userAction:    'Ask customer to verify their billing address.',
    },
    'PAYER_CANNOT_PAY': {
        category:      'ACTION_REQUIRED',
        retryable:     false,
        reason:        'PayPal: Payer account is restricted or cannot complete payment.',
        retryDelaysMs: [],
        userAction:    'Ask customer to log in to PayPal and resolve account restrictions.',
    },
};


// ─────────────────────────────────────────────────────────────────
// GATEWAY-AWARE PROFILE RESOLUTION
// ─────────────────────────────────────────────────────────────────

/**
 * Resolve a decline code to its full profile for a given provider.
 *
 * @param {string|undefined}              code
 * @param {'stripe'|'adyen'|'paypal'}     [provider='stripe']
 * @returns {DeclineProfile}
 */
const resolveDeclineProfile = (code, provider = 'stripe') => {
    if (!code) return _unknownProfile('no_decline_code');
    switch (provider) {
        case 'adyen':  return ADYEN_DECLINE_CODE_PROFILES[code]  ?? _unknownProfile(code);
        case 'paypal': return PAYPAL_DECLINE_CODE_PROFILES[code] ?? _unknownProfile(code);
        case 'stripe':
        default:       return DECLINE_CODE_PROFILES[code]        ?? _unknownProfile(code);
    }
};

/** @param {string} code @returns {DeclineProfile} */
const _unknownProfile = (code) => ({
    category:      'SOFT_FAIL',
    retryable:     true,
    reason:        `Unrecognised decline code: ${code} — defaulting to soft fail`,
    retryDelaysMs: [5 * 60 * 1000, 30 * 60 * 1000, 2 * 60 * 60 * 1000, 24 * 60 * 60 * 1000],
    userAction:    null,
});


// ─────────────────────────────────────────────────────────────────
// SMART RETRY DELAYS
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
// PROVIDER-SPECIFIC PAYLOAD NORMALISATION
//
// Each provider sends webhook events with a completely different
// shape. normalisePayload() extracts a consistent internal
// structure so the rest of the engine never branches on provider.
//
// Stripe  — charge / payment_intent events
// Adyen   — NotificationRequestItem
// PayPal  — Webhooks v2 JSON (Orders / Payments API)
// ─────────────────────────────────────────────────────────────────

/**
 * @typedef {Object} NormalisedPayload
 * @property {string}  declineCode         — provider-normalised taxonomy key
 * @property {string}  cardCountry         — ISO 3166-1 alpha-2, or 'unknown'
 * @property {string}  cardBrand           — e.g. 'visa', 'mastercard', or 'unknown'
 * @property {string}  issuer              — BIN/issuer identifier, or 'unknown'
 * @property {boolean} requires3DS         — issuer explicitly requires authentication
 * @property {boolean} hasInvoice          — charge belongs to a subscription/recurring contract
 * @property {string}  customerId          — provider customer/payer identifier
 * @property {number}  amount              — in smallest currency unit (cents / pence)
 * @property {string}  eventType           — normalised event type string for priority lookup
 */

/**
 * @param {Object}                    payload  Raw webhook event body
 * @param {'stripe'|'adyen'|'paypal'} provider
 * @returns {NormalisedPayload}
 */
const normalisePayload = (payload, provider) => {
    switch (provider) {

        // ── Stripe ────────────────────────────────────────────────
        case 'stripe': {
            const obj        = payload?.data?.object ?? {};
            const piObj      = obj?.payment_intent   ?? {};
            const outcomeObj = obj?.outcome           ?? {};
            const card       = obj?.payment_method_details?.card ?? {};

            return {
                declineCode:  outcomeObj?.network_decline_code ?? outcomeObj?.reason ?? 'unknown',
                cardCountry:  card?.country ?? 'unknown',
                cardBrand:    card?.brand   ?? 'unknown',
                issuer:       card?.issuer  ?? 'unknown',
                // Stripe 3DS: payment_intent has a pending next_action
                requires3DS:  !!(
                    piObj?.next_action?.type === 'use_stripe_sdk' ||
                    piObj?.next_action?.type === 'redirect_to_url'
                ),
                hasInvoice:   !!obj?.invoice,
                customerId:   obj?.customer ?? 'unknown',
                amount:       obj?.amount   ?? 0,
                eventType:    payload?.type ?? '',
            };
        }

        // ── Adyen ─────────────────────────────────────────────────
        // https://docs.adyen.com/development-resources/webhooks/understand-notifications
        case 'adyen': {
            // Support both the raw container and a pre-unwrapped item
            const item       = payload?.NotificationRequestItem ?? payload ?? {};
            const additional = item?.additionalData ?? {};

            const rawReason = item?.reason ?? additional?.refusalReasonCode ?? 'unknown';

            // 3DS detection: use explicit SCA signals only.
            // DO NOT use threeDAuthenticated or liabilityShift alone:
            //   - threeDAuthenticated === 'false'  → auth attempted + failed (≠ required)
            //   - liabilityShift === 'false'        → post-auth liability indicator, not a requirement
            const explicit3DSReason = typeof rawReason === 'string' &&
                rawReason.toLowerCase().includes('3d secure authentication required');
            const scaRequired       = additional?.scaRequired === 'true';
            const authResultCode    = additional?.authenticationResultCode ?? '';
            // threeDOffered=true + no authResultCode → 3DS offered but not completed
            const requires3DS = explicit3DSReason || scaRequired ||
                (additional?.threeDOffered === 'true' && authResultCode === '');

            const declineCode = requires3DS ? 'ADYEN_3DS_REQUIRED' : rawReason;

            // Recurring/subscription: shopperInteraction ContAuth or a stored recurringModel
            const hasInvoice = !!(
                additional?.recurringProcessingModel ||
                additional?.shopperInteraction === 'ContAuth'
            );

            return {
                declineCode,
                cardCountry:  additional?.issuerCountry ?? 'unknown',
                cardBrand:    additional?.paymentMethodVariant ?? item?.paymentMethod ?? 'unknown',
                issuer:       additional?.issuerBin ?? 'unknown',
                requires3DS,
                hasInvoice,
                customerId:   item?.merchantReference ?? additional?.shopperReference ?? 'unknown',
                // Adyen amount.value is always an integer in minor units
                amount:       parseInt(item?.amount?.value ?? '0', 10),
                eventType:    item?.eventCode ?? '',
            };
        }

        // ── PayPal ────────────────────────────────────────────────
        // https://developer.paypal.com/docs/api/webhooks/v1/
        case 'paypal': {
            const resource      = payload?.resource ?? {};
            const payer         = resource?.payer   ?? {};
            const card          = resource?.payment_source?.card ?? {};
            const processorResp = resource?.processor_response  ?? {};

            const rawCode    = processorResp?.response_code ?? resource?.status ?? 'unknown';
            const declineCode = _mapPaypalResponseCode(rawCode);

            // PayPal 3DS: canonical signal is enrollment_status + authentication_status.
            // liability_shift=POSSIBLE is a post-auth liability flag — NOT a 3DS requirement.
            const threeDResult         = card?.authentication_result?.three_d_secure ?? {};
            const enrollmentStatus     = threeDResult?.enrollment_status     ?? '';  // Y|N|U
            const authenticationStatus = threeDResult?.authentication_status ?? '';  // Y|N|A|U|REQUIRED
            const requires3DS = (
                enrollmentStatus === 'Y' &&
                (authenticationStatus === 'REQUIRED' || authenticationStatus === 'N')
            );

            // Subscriptions: billing agreement or subscription resource
            const hasInvoice = !!(resource?.billing_agreement_id || resource?.subscription_id);

            return {
                declineCode,
                cardCountry:  card?.billing_address?.country_code ?? 'unknown',
                cardBrand:    card?.brand ?? 'unknown',
                issuer:       card?.bin   ?? 'unknown',
                requires3DS,
                hasInvoice,
                customerId:   payer?.payer_id ?? resource?.id ?? 'unknown',
                // PayPal amounts are decimal strings (e.g. "10.99") → convert to cents
                amount:       Math.round(parseFloat(resource?.amount?.value ?? '0') * 100),
                eventType:    payload?.event_type ?? '',
            };
        }

        default:
            return {
                declineCode: 'unknown',
                cardCountry: 'unknown',
                cardBrand:   'unknown',
                issuer:      'unknown',
                requires3DS: false,
                hasInvoice:  false,
                customerId:  'unknown',
                amount:      0,
                eventType:   '',
            };
    }
};

/**
 * Map raw PayPal processor response codes to PAYPAL_DECLINE_CODE_PROFILES keys.
 * PayPal surfaces ISO-8583 numeric codes inside processor_response.response_code.
 * Unknown codes fall through as-is → _unknownProfile() in the caller.
 *
 * @param {string} rawCode
 * @returns {string}
 */
const _mapPaypalResponseCode = (rawCode) => {
    const MAP = {
        // ISO-8583 numeric → taxonomy key
        '05': 'DO_NOT_HONOR',
        '14': 'INVALID_ACCOUNT',
        '15': 'INVALID_ACCOUNT',
        '41': 'DO_NOT_HONOR',           // lost card
        '43': 'DO_NOT_HONOR',           // stolen card
        '51': 'INSUFFICIENT_FUNDS',
        '54': 'EXPIRED_CARD',
        '57': 'CARD_TYPE_NOT_SUPPORTED',
        '62': 'CARD_TYPE_NOT_SUPPORTED',
        '96': 'PAYPAL_INTERNAL_ERROR',
        'N7': 'INVALID_CVV2',
        // PayPal-native string codes — identity mapping
        'INSUFFICIENT_FUNDS':      'INSUFFICIENT_FUNDS',
        'DO_NOT_HONOR':            'DO_NOT_HONOR',
        'CARD_CLOSED':             'CARD_CLOSED',
        'PAYPAL_RISK_DECLINE':     'PAYPAL_RISK_DECLINE',
        'CURRENCY_NOT_SUPPORTED':  'CURRENCY_NOT_SUPPORTED',
        'CARD_TYPE_NOT_SUPPORTED': 'CARD_TYPE_NOT_SUPPORTED',
        'EXPIRED_CARD':            'EXPIRED_CARD',
        'INVALID_CVV2':            'INVALID_CVV2',
        'INVALID_ACCOUNT':         'INVALID_ACCOUNT',
        'AVS_MISMATCH':            'AVS_MISMATCH',
        'PAYER_CANNOT_PAY':        'PAYER_CANNOT_PAY',
        'TRY_AGAIN_LATER':         'TRY_AGAIN_LATER',
        'PAYPAL_LIMIT_EXCEEDED':   'PAYPAL_LIMIT_EXCEEDED',
    };
    return MAP[rawCode] ?? rawCode;
};


// ─────────────────────────────────────────────────────────────────
// RCA PATTERN DETECTION — do_not_honor (all providers)
//
// Each provider has a different code for "generic hard decline".
// We bucket hits per (gatewayId, cardCountry, cardBrand, issuer)
// in Redis with a fixed TTL window.
//
// ≥ DNH_PATTERN_THRESHOLD hits in DNH_WINDOW_SECONDS
//   → upgrade that decline to HARD_FAIL regardless of taxonomy.
// ─────────────────────────────────────────────────────────────────

/** The decline code that maps to "do not honor" for each provider. */
const DNH_SENTINEL_BY_PROVIDER = {
    stripe: 'do_not_honor',
    adyen:  '27',
    paypal: 'DO_NOT_HONOR',
};

const DNH_PATTERN_THRESHOLD = 3;
const DNH_WINDOW_SECONDS    = 600; // 10 minutes

const _dnhKey = ({ gatewayId, cardCountry, cardBrand, issuer }) =>
    `dnh_pattern:${gatewayId}:${cardCountry}:${cardBrand}:${issuer}`;

/**
 * Record a DNH hit and detect whether it is becoming a pattern.
 * TTL is set only on the first increment — the window is fixed, not sliding.
 *
 * @returns {Promise<{ isPattern: boolean, count: number }>}
 */
const recordDNHPattern = async ({ gatewayId, cardCountry, cardBrand, issuer }) => {
    try {
        const key   = _dnhKey({ gatewayId, cardCountry, cardBrand, issuer });
        const count = await redis.incr(key);
        // Set TTL only on first hit — avoids a sliding-window race condition
        // where concurrent workers keep extending the window indefinitely.
        if (count === 1) await redis.expire(key, DNH_WINDOW_SECONDS);

        if (count >= DNH_PATTERN_THRESHOLD) {
            console.warn(
                `[retryDecisionEngine] ⚠️ DNH pattern detected: gateway=${gatewayId}, ` +
                `country=${cardCountry}, brand=${cardBrand}, issuer=${issuer} — count=${count}`
            );
            try {
                await db.query(
                    `INSERT INTO decline_patterns
                         (gateway_id, card_country, card_brand, issuer, pattern_count, detected_at)
                     VALUES ($1,$2,$3,$4,$5,NOW())
                     ON CONFLICT (gateway_id, card_country, card_brand, issuer)
                     DO UPDATE SET pattern_count = $5, detected_at = NOW()`,
                    [gatewayId, cardCountry, cardBrand, issuer, count]
                );
            } catch (dbErr) {
                console.error('[retryDecisionEngine] Failed to persist DNH pattern to DB:', dbErr?.message);
            }
            return { isPattern: true, count };
        }
        return { isPattern: false, count };
    } catch (redisErr) {
        console.error('[retryDecisionEngine] Redis error in recordDNHPattern:', redisErr?.message);
        return { isPattern: false, count: 0 }; // fail open
    }
};

/**
 * Check whether a known DNH pattern exists for this combination (read-only).
 *
 * @returns {Promise<boolean>}
 */
const isDNHPatternKnown = async ({ gatewayId, cardCountry, cardBrand, issuer }) => {
    try {
        const val   = await redis.get(_dnhKey({ gatewayId, cardCountry, cardBrand, issuer }));
        const count = parseInt(val ?? '0', 10);
        return count >= DNH_PATTERN_THRESHOLD;
    } catch (redisErr) {
        console.error('[retryDecisionEngine] Redis error in isDNHPatternKnown:', redisErr?.message);
        return false; // fail open — don't block on Redis being down
    }
};


// ─────────────────────────────────────────────────────────────────
// SUCCESS PROBABILITY SCORING
//
// Score 0–1 before scheduling any retry.
// If score < SUCCESS_PROBABILITY_THRESHOLD → DLQ, don't retry.
// Blends a Bayesian prior (category + attempt) with real historical
// data, weighted by sample size — small samples trust the prior more.
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
 * @param {number}         p.attemptNumber    1-based
 * @param {boolean}        p.isDNHPattern
 * @param {number|null}    p.historicalRate   null = insufficient data
 * @param {number|null}    p.historicalTotal  sample count backing historicalRate
 * @returns {number} 0.0 – 1.0
 */
const computeSuccessProbability = ({ profile, attemptNumber, isDNHPattern, historicalRate, historicalTotal }) => {
    const idx      = Math.min(attemptNumber - 1, 3);
    const base     = BASE_SUCCESS_PROBABILITY[profile.category]?.[idx] ?? 0.05;
    const prior    = isDNHPattern ? base * 0.5 : base;

    if (historicalRate !== null && historicalRate !== undefined) {
        // Weight real data proportionally to sample count; saturates at 1.0 by 100 samples.
        // Prevents noisy low-sample rates from dominating the prior.
        const dataWeight  = Math.min((historicalTotal ?? 0) / 100, 1);
        const priorWeight = 1 - dataWeight;
        return dataWeight * historicalRate + priorWeight * prior;
    }
    return prior;
};

/**
 * Fetch historical success rate for a (declineCode, provider) pair from the last 30 days.
 * Provider-scoped so Adyen '27' and Stripe 'do_not_honor' don't pollute each other's stats.
 *
 * @param {string}                    declineCode
 * @param {'stripe'|'adyen'|'paypal'} provider
 * @returns {Promise<{ rate: number, total: number } | null>}
 */
const fetchHistoricalSuccessRate = async (declineCode, provider = 'stripe') => {
    try {
        const res = await db.query(
            `SELECT success_count, failure_count
             FROM decline_code_stats
             WHERE decline_code = $1
               AND provider     = $2`,
            [declineCode, provider]
        );

        const row = res.rows[0];
        if (!row) return null;

        const successes = parseInt(row.success_count, 10);
        const failures  = parseInt(row.failure_count, 10);
        const total     = successes + failures;

        if (total < MIN_HISTORICAL_SAMPLES) return null;

        return { rate: successes / total, total };

    } catch (dbErr) {
        console.error('[retryDecisionEngine] DB error in fetchHistoricalSuccessRate:', dbErr?.message);
        return null;
    }
};

/**
 * Record a retry outcome so future probability calculations improve over time.
 *
 * @param {string}                          declineCode
 * @param {'SUCCESS'|'FAILED'|'HARD_FAIL'}  outcome
 * @param {'stripe'|'adyen'|'paypal'}       provider
 */
const recordDeclineCodeOutcome = async (declineCode, outcome, provider = 'stripe') => {
    try {
        await db.query(
            `INSERT INTO decline_code_stats (decline_code, provider, success_count, failure_count)
             VALUES ($1, $2,
                 CASE WHEN $3 = 'SUCCESS' THEN 1 ELSE 0 END,
                 CASE WHEN $3 != 'SUCCESS' THEN 1 ELSE 0 END
             )
             ON CONFLICT (decline_code, provider) DO UPDATE SET
                 success_count = decline_code_stats.success_count + CASE WHEN $3 = 'SUCCESS' THEN 1 ELSE 0 END,
                 failure_count = decline_code_stats.failure_count + CASE WHEN $3 != 'SUCCESS' THEN 1 ELSE 0 END,
                 last_updated  = NOW()`,
            [declineCode, provider, outcome]
        );
    } catch (dbErr) {
        console.error('[retryDecisionEngine] DB error in recordDeclineCodeOutcome:', dbErr?.message);
    }
};


// ─────────────────────────────────────────────────────────────────
// CROSS-EVENT JOURNEY INTELLIGENCE
//
// Tracks the full payment lifecycle per customer in Redis so the
// retry worker has full context across multiple webhook events:
//   • Is there a subscription at risk?
//   • How many times has this invoice failed?
//   • Is 3DS required?
//   • What is the urgency level?
//
// Journey keys are scoped: journey:{gatewayId}:{provider}:{customerId}
// so Adyen and Stripe customers with the same ID string never collide.
// ─────────────────────────────────────────────────────────────────

/**
 * @typedef {Object} JourneyContext
 * @property {boolean}                          hasActiveSubscription
 * @property {boolean}                          isSubscriptionPastDue
 * @property {boolean}                          isSubscriptionAtRisk   — 3+ failed attempts on a subscription
 * @property {boolean}                          requires3DS
 * @property {string}                           declineCode
 * @property {string}                           cardCountry
 * @property {string}                           cardBrand
 * @property {string}                           issuer
 * @property {number}                           journeyAttemptCount
 * @property {'LOW'|'MEDIUM'|'HIGH'|'CRITICAL'} urgency
 * @property {string}                           eventType
 */

const JOURNEY_TTL_SECONDS = 30 * 24 * 60 * 60; // 30 days — covers the longest retry window

/**
 * Build full journey context from a raw webhook payload + any prior Redis state.
 *
 * @param {Object}                    p
 * @param {string|number}             p.gatewayId
 * @param {Object}                    p.payload
 * @param {'stripe'|'adyen'|'paypal'} p.provider
 * @returns {Promise<JourneyContext>}
 */
const buildJourneyContext = async ({ gatewayId, payload, provider = 'stripe' }) => {
    const norm       = normalisePayload(payload, provider);
    const journeyKey = `journey:${gatewayId}:${provider}:${norm.customerId}`;

    let priorState = {};
    try {
        const raw = await redis.get(journeyKey);
        if (raw) priorState = JSON.parse(raw);
    } catch (redisErr) {
        console.error('[retryDecisionEngine] Redis error reading journey state:', redisErr?.message);
    }

    const journeyAttemptCount = (priorState.attemptCount ?? 0) + 1;
    const isPastDue           = priorState.subscriptionStatus === 'past_due';
    const isHighValue         = norm.amount > 10_000; // > $100

    let urgency = 'LOW';
    if (journeyAttemptCount >= 3 && norm.hasInvoice) urgency = 'CRITICAL';
    else if (isPastDue || isHighValue)               urgency = 'HIGH';
    else if (journeyAttemptCount >= 2)               urgency = 'MEDIUM';

    try {
        await redis.set(
            journeyKey,
            JSON.stringify({
                ...priorState,
                declineCode:        norm.declineCode,
                cardCountry:        norm.cardCountry,
                cardBrand:          norm.cardBrand,
                issuer:             norm.issuer,
                requires3DS:        norm.requires3DS,
                attemptCount:       journeyAttemptCount,
                lastFailedAt:       new Date().toISOString(),
                subscriptionStatus: priorState.subscriptionStatus ?? 'unknown',
            }),
            'EX', JOURNEY_TTL_SECONDS
        );
    } catch (redisErr) {
        console.error('[retryDecisionEngine] Redis error persisting journey state:', redisErr?.message);
    }

    return {
    hasActiveSubscription: norm.hasInvoice,
    isSubscriptionPastDue: isPastDue,
    isSubscriptionAtRisk:  journeyAttemptCount >= 3 && norm.hasInvoice,
    requires3DS:           norm.requires3DS,
    declineCode:           norm.declineCode,
    cardCountry:           norm.cardCountry,
    cardBrand:             norm.cardBrand,
    issuer:                norm.issuer,
    journeyAttemptCount,
    urgency,
    eventType:             norm.eventType,
    amount:                norm.amount, // ✅ FIX
};
};

/**
 * Update subscription status in the journey store.
 * Call from webhookController on:
 *   Stripe:  customer.subscription.updated / deleted
 *   Adyen:   RECURRING_CONTRACT notification
 *   PayPal:  BILLING.SUBSCRIPTION.UPDATED / CANCELLED
 *
 * @param {Object}                    p
 * @param {string|number}             p.gatewayId
 * @param {string}                    p.customerId   Provider customer/payer ID
 * @param {string}                    p.status       Provider subscription status string
 * @param {'stripe'|'adyen'|'paypal'} p.provider
 */
const updateJourneySubscriptionStatus = async ({ gatewayId, customerId, status, provider = 'stripe' }) => {
    const journeyKey = `journey:${gatewayId}:${provider}:${customerId}`;
    try {
        const raw   = await redis.get(journeyKey);
        const state = raw ? JSON.parse(raw) : {};
        state.subscriptionStatus = status;
        await redis.set(journeyKey, JSON.stringify(state), 'EX', JOURNEY_TTL_SECONDS);
    } catch (redisErr) {
        console.error('[retryDecisionEngine] Redis error in updateJourneySubscriptionStatus:', redisErr?.message);
    }
};


// ─────────────────────────────────────────────────────────────────
// EVENT PRIORITY TABLE
//
// Single source of truth shared with retryQueue.js via exports.
// Both the engine and the queue use this — no duplication.
// ─────────────────────────────────────────────────────────────────

const EVENT_PRIORITY = {
    // Stripe
    'charge.failed':                  10,
    'payment_intent.payment_failed':  10,
    'charge.dispute.created':          7,
    'customer.subscription.deleted':   5,
    // Adyen
    'AUTHORISATION':                  10,
    'CHARGEBACK':                      7,
    'CANCEL_OR_REFUND':                3,
    // PayPal
    'PAYMENT.SALE.DENIED':            10,
    'PAYMENT.CAPTURE.DENIED':         10,
    'BILLING.SUBSCRIPTION.SUSPENDED':  7,
    'BILLING.SUBSCRIPTION.CANCELLED':  5,
};

const URGENCY_PRIORITY_BONUS = { CRITICAL: 5, HIGH: 3, MEDIUM: 1, LOW: 0 };


// ─────────────────────────────────────────────────────────────────
// MAIN EXPORT — makeRetryDecision
//
// The single function the infra layer calls. Returns a plain
// decision object — no side effects on queues.
// Backwards compatible: provider defaults to 'stripe'.
// ─────────────────────────────────────────────────────────────────

/**
 * @typedef {Object} RetryDecision
 * @property {'RETRY'|'HARD_STOP'|'ACTION_REQUIRED'|'LOW_PROBABILITY_STOP'} action
 * @property {DeclineProfile}  profile
 * @property {JourneyContext}  journeyContext
 * @property {number}          delayMs
 * @property {number}          priority
 * @property {number}          successProbability
 * @property {string|null}     userAction
 * @property {string}          reason
 * @property {string}          provider
 */

/**
 * @param {Object}                    p
 * @param {string|number}             p.gatewayId
 * @param {string}                    p.declineCode
 * @param {number}                    p.attemptNumber     1-based
 * @param {Object}                    p.payload           Raw webhook event body
 * @param {'stripe'|'adyen'|'paypal'} [p.provider='stripe']
 * @returns {Promise<RetryDecision>}
 */
const makeRetryDecision = async ({
    gatewayId,
    declineCode,
    attemptNumber,
    payload,
    provider = 'stripe',
}) => {

    // ── 1. Resolve baseline profile (provider-scoped) ─────────────
    const baseProfile = resolveDeclineProfile(declineCode, provider);

    // ── 2. Build journey context (normalises the raw payload) ──────
    const journeyContext = await buildJourneyContext({ gatewayId, payload, provider });

    // ── 3. Gather override signals — collect all, apply once ───────
    //    Evaluating signals separately before touching the profile
    //    eliminates order-dependent mutation bugs.

    // Signal A: issuer demands 3DS authentication
    const requires3DS = journeyContext.requires3DS;

    // Signal B: systematic DNH pattern detected for this BIN cluster
    let isDNHPattern = false;
    const dnhSentinel = DNH_SENTINEL_BY_PROVIDER[provider] ?? 'do_not_honor';
    if (declineCode === dnhSentinel) {
        const { isPattern } = await recordDNHPattern({
            gatewayId,
            cardCountry: journeyContext.cardCountry,
            cardBrand:   journeyContext.cardBrand,
            issuer:      journeyContext.issuer,
        });
        isDNHPattern = isPattern;
    }

    // ── 4. Compute final profile — single derivation, explicit precedence ─
    //    3DS beats DNH: if the issuer wants authentication it wins.
    const profile = { ...baseProfile };
    if (requires3DS) {
        profile.category   = 'ACTION_REQUIRED';
        profile.retryable  = false;
        profile.userAction = 'Trigger 3DS authentication flow — do not blindly retry.';
    } else if (isDNHPattern) {
        profile.category  = 'HARD_FAIL';
        profile.retryable = false;
    }

    // ── 5. Success probability (provider-scoped historical data) ───
    const historical         = await fetchHistoricalSuccessRate(declineCode, provider);
    const successProbability = computeSuccessProbability({
        profile,
        attemptNumber,
        isDNHPattern,
        historicalRate:  historical?.rate  ?? null,
        historicalTotal: historical?.total ?? null,
    });

    // ── 6. Final decision ──────────────────────────────────────────
    const basePriority = EVENT_PRIORITY[journeyContext.eventType] ?? 5; // neutral midpoint
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
            provider,
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
            provider,
        };
    }

    return {
        action:            'RETRY',
        profile,
        journeyContext,
        delayMs:           withJitter(getSmartDelay(attemptNumber, profile)),
        priority,
        successProbability,
        userAction:        null,
        reason:            profile.reason,
        provider,
    };
};


// ─────────────────────────────────────────────────────────────────
// EXPORTS
// ─────────────────────────────────────────────────────────────────

module.exports = {
    // Primary API — called by retryQueue
    makeRetryDecision,
    recordDeclineCodeOutcome,
    updateJourneySubscriptionStatus,
    buildJourneyContext,

    // Shared constants — retryQueue imports these to avoid duplication
    EVENT_PRIORITY,
    URGENCY_PRIORITY_BONUS,
    DECLINE_CODE_PROFILES,
    ADYEN_DECLINE_CODE_PROFILES,
    PAYPAL_DECLINE_CODE_PROFILES,
    SUCCESS_PROBABILITY_THRESHOLD,

    // Exported for unit testing
    resolveDeclineProfile,
    computeSuccessProbability,
    normalisePayload,
    isDNHPatternKnown,
};