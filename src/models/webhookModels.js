const db = require('../config/db');

const logWebhook = async (data) => {
    const {
        gateway_id,
        provider_event_id,
        event_type,
        status,
        http_status,
        payload,
        error_stack,
        last_error_message,
        latency
    } = data;

 const query = `
    INSERT INTO webhook_logs (
        gateway_id, 
        provider_event_id, 
        event_type, 
        status,
        original_status,
        http_status_code, 
        payload, 
        error_stack, 
        last_error_message, 
        latency_ms
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (provider_event_id, gateway_id)
    DO UPDATE SET 
        status             = EXCLUDED.status,
        error_stack        = EXCLUDED.error_stack,
        last_error_message = EXCLUDED.last_error_message,
        http_status_code   = EXCLUDED.http_status_code,
        latency_ms         = EXCLUDED.latency_ms
        -- original_status intentionally excluded — never overwritten
    RETURNING id;
`;

const values = [
    gateway_id,
    provider_event_id,
    event_type,
    status,        // $4 → status
    status,        // $5 → original_status (same value, own slot)
    http_status,   // $6
    JSON.stringify(payload), // $7
    error_stack,   // $8
    last_error_message, // $9
    latency || 0,  // $10
];

    try {
        const res = await db.query(query, values);
        return res.rows[0];
    } catch (err) {
        console.error('❌ Database Logic Error:', err.message);
        throw err;
    }
};


const getAnalyticsByGateway = async (gateway_id) => {
    const query = `
        SELECT 
            COUNT(*) FILTER (WHERE status != 'IGNORED')::int                    AS total_count,
            COUNT(*) FILTER (WHERE status = 'SUCCESS')::int                     AS success_count,
            COUNT(*) FILTER (WHERE status IN ('FAILED', 'CRITICAL'))::int       AS failure_count,
            CASE 
                WHEN COUNT(*) FILTER (WHERE status != 'IGNORED') > 0 THEN 
                    ROUND(
                        COUNT(*) FILTER (WHERE status = 'SUCCESS')::float 
                        / COUNT(*) FILTER (WHERE status != 'IGNORED') * 100
                    )
                ELSE 0 
            END AS success_percentage,
            (
                SELECT json_agg(t) FROM (
                    SELECT last_error_message, COUNT(*)::int AS occurrence
                    FROM webhook_logs
                    WHERE gateway_id = $1
                    AND status IN ('FAILED', 'CRITICAL')
                    AND last_error_message IS NOT NULL
                    AND last_error_message != ''
                    GROUP BY last_error_message
                    ORDER BY occurrence DESC
                    LIMIT 3
                ) t
            ) AS error_breakdown
        FROM webhook_logs
        WHERE gateway_id = $1
        AND status != 'IGNORED';
    `;

    const res = await db.query(query, [gateway_id]);
    return res.rows[0];
};

module.exports = { logWebhook, getAnalyticsByGateway };