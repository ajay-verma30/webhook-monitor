const stripe = require("stripe");
const db = require("../config/db");

const verifyStripeSignature = async (req) => {
  const { gateway_id } = req.params;
  const sig = req.headers["stripe-signature"];

  const result = await db.query(
    "SELECT webhook_secret FROM gateways WHERE id = $1",
    [gateway_id],
  );

  if (!result.rows.length) {
    throw new Error("Gateway Missing");
  }

  const endpointSecret = result.rows[0].webhook_secret;

  const event = stripe.webhooks.constructEvent(
    req.rawBody,
    sig,
    endpointSecret,
  );

  req.stripeEvent = event;
};

module.exports = { verifyStripeSignature };
