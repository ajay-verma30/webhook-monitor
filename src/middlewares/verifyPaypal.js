'use strict';

const axios = require('axios');
const db    = require('../config/db');

/**
 * PayPal does NOT use a simple HMAC like Stripe or Adyen.
 * Instead, it signs webhooks with a certificate chain (CRC32 + RSA-SHA256).
 *
 * The simplest and most reliable approach is to call PayPal's own
 * /v1/notifications/verify-webhook-signature endpoint, which handles
 * all certificate validation server-side.
 *
 * Required headers PayPal sends with every webhook:
 *   paypal-auth-algo        e.g. "SHA256withRSA"
 *   paypal-cert-url         URL of the signing certificate
 *   paypal-transmission-id  Unique transmission ID
 *   paypal-transmission-sig Base64-encoded RSA signature
 *   paypal-transmission-time ISO8601 timestamp
 *
 * PayPal docs:
 * https://developer.paypal.com/api/rest/webhooks/rest/#link-verifywebhooksignature
 */

/**
 * Fetches a short-lived PayPal OAuth2 access token using client credentials.
 * Uses the PAYPAL_CLIENT_ID and PAYPAL_CLIENT_SECRET environment variables.
 *
 * @returns {Promise<string>} Bearer access token
 */
const getPaypalAccessToken = async () => {
    const clientId     = process.env.PAYPAL_CLIENT_ID;
    const clientSecret = process.env.PAYPAL_CLIENT_SECRET;
    const baseUrl      = process.env.PAYPAL_BASE_URL ?? 'https://api-m.sandbox.paypal.com';

    if (!clientId || !clientSecret) {
        throw new Error('PAYPAL_CLIENT_ID and PAYPAL_CLIENT_SECRET must be set.');
    }

    const response = await axios.post(
        `${baseUrl}/v1/oauth2/token`,
        'grant_type=client_credentials',
        {
            auth:    { username: clientId, password: clientSecret },
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            timeout: 5_000,
        },
    );

    return response.data.access_token;
};

const verifyPaypalSignature = async (req, res, next) => {
    const { gateway_id } = req.params;

    // PayPal's required verification headers
    const transmissionId   = req.headers['paypal-transmission-id'];
    const transmissionTime = req.headers['paypal-transmission-time'];
    const certUrl          = req.headers['paypal-cert-url'];
    const authAlgo         = req.headers['paypal-auth-algo'];
    const transmissionSig  = req.headers['paypal-transmission-sig'];

    if (!transmissionId || !transmissionTime || !certUrl || !authAlgo || !transmissionSig) {
        return res.status(400).send('Webhook Error: Missing PayPal signature headers.');
    }

    try {
        const result = await db.query(
            'SELECT webhook_secret FROM gateways WHERE id = $1',
            [gateway_id],
        );

        if (result.rows.length === 0) return res.status(404).send('Gateway Missing');

        // For PayPal, webhook_secret stores the Webhook ID from the PayPal dashboard
        // (not an HMAC key — PayPal needs its own webhook ID to verify the signature)
        const webhookId = result.rows[0].webhook_secret;

        const accessToken = await getPaypalAccessToken();
        const baseUrl     = process.env.PAYPAL_BASE_URL ?? 'https://api-m.sandbox.paypal.com';

        const verifyResponse = await axios.post(
            `${baseUrl}/v1/notifications/verify-webhook-signature`,
            {
                auth_algo:         authAlgo,
                cert_url:          certUrl,
                transmission_id:   transmissionId,
                transmission_sig:  transmissionSig,
                transmission_time: transmissionTime,
                webhook_id:        webhookId,
                // PayPal requires the raw parsed body (not re-serialised)
                webhook_event:     req.body,
            },
            {
                headers: {
                    Authorization:  `Bearer ${accessToken}`,
                    'Content-Type': 'application/json',
                },
                timeout: 5_000,
            },
        );

        if (verifyResponse.data?.verification_status !== 'SUCCESS') {
            return res.status(400).send('Webhook Error: PayPal signature verification failed.');
        }

        // Attach the verified event for the controller
        req.paypalEvent = req.body;
        next();

    } catch (err) {
        res.status(400).send(`Webhook Error: ${err.message}`);
    }
};

module.exports = { verifyPaypalSignature };