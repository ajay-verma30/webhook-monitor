const db = require('../config/db');
const slugify = require('slugify');

// 🔥 REGISTER GATEWAY
const ALLOWED_PROVIDERS = ['STRIPE', 'ADYEN', 'PAYPAL'];

const registerGateway = async (req, res) => {
  const userId = req.user.id;
  const { name, provider_type, webhook_secret } = req.body;

  try {
    // 🔹 Normalize provider
    const normalizedProvider = provider_type?.toUpperCase();

    // 🔹 Validate provider (IMPORTANT)
    if (!ALLOWED_PROVIDERS.includes(normalizedProvider)) {
      return res.status(400).json({
        error: `Invalid provider_type. Allowed: ${ALLOWED_PROVIDERS.join(', ')}`
      });
    }

    // 🔹 Check duplicate name (per user)
    const checkName = await db.query(
      'SELECT id FROM gateways WHERE name = $1 AND user_id = $2',
      [name, userId]
    );

    if (checkName.rows.length > 0) {
      return res.status(400).json({
        error: "Gateway name already exists"
      });
    }

    // 🔹 Check webhook secret
    const checkSecret = await db.query(
      'SELECT id FROM gateways WHERE webhook_secret = $1',
      [webhook_secret]
    );

    if (checkSecret.rows.length > 0) {
      return res.status(400).json({
        error: "Webhook secret already exists"
      });
    }

    // 🔥 SLUG GENERATION
    const baseSlug = slugify(name, { lower: true, strict: true });

    let slug = baseSlug;
    let counter = 1;

    while (true) {
      const checkSlug = await db.query(
        'SELECT id FROM gateways WHERE slug = $1',
        [slug]
      );

      if (checkSlug.rows.length === 0) break;

      slug = `${baseSlug}-${counter}`;
      counter++;
    }

    // 🔥 INSERT (ENUM SAFE NOW)
    const result = await db.query(
      `INSERT INTO gateways (name, provider_type, webhook_secret, user_id, slug)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, slug`,
      [name, normalizedProvider, webhook_secret, userId, slug]
    );
    
    return res.status(201).json({
    id:   result.rows[0].id,
    slug: result.rows[0].slug,
    url:  `http://localhost:3000/api/v1/webhooks/${normalizedProvider.toLowerCase()}/${result.rows[0].id}`
});

  } catch (err) {
    console.error("REGISTER GATEWAY ERROR:", err);

    // 🔥 Handle enum violation fallback (extra safety)
    if (err.code === '22P02') {
      return res.status(400).json({
        error: "Invalid provider_type (enum violation)"
      });
    }

    return res.status(500).json({
      error: err.message || "Failed to register gateway"
    });
  }
};

// 🔥 GET GATEWAYS
const getGateways = async (req, res) => {
  try {
    const userId = req.user.id;

    const result = await db.query(
      `SELECT id, name, slug, provider_type, created_at 
       FROM gateways 
       WHERE user_id = $1 
       ORDER BY created_at DESC`,
      [userId]
    );

    return res.status(200).json({
      gateways: result.rows
    });

  } catch (err) {
    return res.status(500).json({
      message: "Failed to fetch gateways",
      error: err.message
    });
  }
};

module.exports = { registerGateway, getGateways };