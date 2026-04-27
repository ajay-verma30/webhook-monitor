
const stripe = require('stripe'); 
const db = require('../config/db');

const verifyStripeSignature = async (req, res, next) => {
    const { gateway_id } = req.params;
    const sig = req.headers['stripe-signature'];

    try {
        const result = await db.query(
            'SELECT webhook_secret FROM gateways WHERE id = $1', 
            [gateway_id]
        );


        if (result.rows.length === 0) return res.status(404).send('Gateway Missing');

        const endpointSecret = result.rows[0].webhook_secret;

        const event = stripe.webhooks.constructEvent(req.rawBody, sig, endpointSecret);

        req.stripeEvent = event;
        next();
    } catch (err) {
        res.status(400).send(`Webhook Error: ${err.message}`);
    }
};


module.exports = {verifyStripeSignature}