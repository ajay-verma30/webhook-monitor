'use strict';

const db = require('../config/db');

// ─── GET /gateways ────────────────────────────────────────────────────────────
/**
 * Returns all gateways belonging to the authenticated user only.
 * The WHERE user_id = $1 clause makes it impossible to leak another user's data.
 */
const getUserGateways = async (req, res) => {
    try {
        const { rows } = await db.query(
            `SELECT id, name, provider_type, webhook_url, slug, is_active, created_at
             FROM gateways
             WHERE user_id = $1
             ORDER BY created_at DESC`,
            [req.user.id],   // 👈 comes from authToken middleware
        );

        return res.status(200).json({ gateways: rows });

    } catch (err) {
        console.error('❌ getUserGateways error:', err.message);
        return res.status(500).json({ error: 'Failed to fetch gateways.' });
    }
};

// ─── GET /gateways/:id ────────────────────────────────────────────────────────
/**
 * Returns a single gateway — but ONLY if it belongs to the requesting user.
 * If the gateway exists but belongs to someone else, we return 404 (not 403),
 * so we don't leak that the gateway ID even exists.
 */
const getGatewayById = async (req, res) => {
    const { id } = req.params;

    try {
        const { rows } = await db.query(
            `SELECT id, name, provider_type, webhook_url, slug, is_active, created_at
             FROM gateways
             WHERE id = $1 AND user_id = $2`,  // 👈 both conditions must match
            [id, req.user.id],
        );

        if (rows.length === 0) {
            return res.status(404).json({ error: 'Gateway not found.' });
        }

        return res.status(200).json({ gateway: rows[0] });

    } catch (err) {
        console.error('❌ getGatewayById error:', err.message);
        return res.status(500).json({ error: 'Failed to fetch gateway.' });
    }
};

module.exports = { getUserGateways, getGatewayById };