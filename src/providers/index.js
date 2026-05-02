// src/providers/index.js

const { verifyStripeSignature } = require('../middlewares/verifyStripe');
const { verifyAdyenSignature }  = require('../middlewares/verifyAdyen');
const { verifyPaypalSignature } = require('../middlewares/verifyPaypal');

const { handleStripeWebhook } = require('../controllers/webhookController');
const { handleAdyenWebhook }  = require('../controllers/adyenController');
const { handlePaypalWebhook } = require('../controllers/paypalController');

const PROVIDERS = {
    stripe: {
        verify: verifyStripeSignature,
        handler: handleStripeWebhook
    },
    adyen: {
        verify: verifyAdyenSignature,
        handler: handleAdyenWebhook
    },                                                                                                  
    paypal: {
        verify: verifyPaypalSignature,
        handler: handlePaypalWebhook
    }
};

module.exports = PROVIDERS;