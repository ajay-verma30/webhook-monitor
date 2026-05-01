'use strict';

const crypto = require('crypto');
const db     = require('../config/db');

/**
 * Adyen uses HMAC-SHA256 to sign each NotificationRequestItem.
 *
 * Verification steps:
 *  1. Pull the HMAC key from the gateways table (stored as hex).
 *  2. Build the signing string from specific fields in the notification item.
 *  3. Compute HMAC-SHA256 and compare against additionalData.hmacSignature.
 *
 * Adyen docs:
 * https://docs.adyen.com/development-resources/webhooks/verify-hmac-signatures
 */

/**
 * Builds the signing string Adyen uses before hashing.
 * Field order is fixed by Adyen's spec — do not reorder.
 *
 * @param {object} item - NotificationRequestItem
 * @returns {string}
 */
const buildSigningString = (item) => {
    const escape = (val) =>
        String(val ?? '').replace(/\\/g, '\\\\').replace(/:/g, '\\:');

    return [
        item.pspReference,
        item.originalReference  ?? '',
        item.merchantAccountCode,
        item.merchantReference,
        item.amount?.value      ?? '',
        item.amount?.currency   ?? '',
        item.eventCode,
        item.success,
    ]
        .map(escape)
        .join(':');
};

/**
 * Verifies the HMAC signature of a single NotificationRequestItem.
 *
 * @param {object} item      - NotificationRequestItem
 * @param {string} hexKey    - HMAC key from gateways.webhook_secret (hex-encoded)
 * @returns {boolean}
 */
const isValidHmac = (item, hexKey) => {
    const signingString    = buildSigningString(item);
    const keyBytes         = Buffer.from(hexKey, 'hex');
    const expected         = crypto
        .createHmac('sha256', keyBytes)
        .update(signingString, 'utf8')
        .digest('base64');

    const received = item.additionalData?.hmacSignature ?? '';

    // Constant-time comparison to prevent timing attacks
    if (expected.length !== received.length) return false;
    return crypto.timingSafeEqual(
        Buffer.from(expected),
        Buffer.from(received),
    );
};

const verifyAdyenSignature = async (req, res, next) => {
    const { gateway_id } = req.params;

    try {
        const result = await db.query(
            'SELECT webhook_secret FROM gateways WHERE id = $1',
            [gateway_id],
        );

        if (result.rows.length === 0) return res.status(404).send('Gateway Missing');

        const hmacKey = result.rows[0].webhook_secret;

        // Adyen sends a batch: { live, notificationItems: [{ NotificationRequestItem }] }
        const items = (req.body?.notificationItems ?? []).map(
            (wrapper) => wrapper.NotificationRequestItem ?? wrapper,
        );

        if (items.length === 0) {
            return res.status(400).send('Webhook Error: No notification items found.');
        }

        // Every item in the batch must pass HMAC verification
        for (const item of items) {
            if (!isValidHmac(item, hmacKey)) {
                return res.status(400).send('Webhook Error: HMAC signature verification failed.');
            }
        }

        // Attach the unwrapped items for the controller
        req.adyenNotifications = items;
        next();

    } catch (err) {
        res.status(400).send(`Webhook Error: ${err.message}`);
    }
};

module.exports = { verifyAdyenSignature };