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
        http_status_code, 
        payload, 
        error_stack, 
        last_error_message, 
        latency_ms
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (provider_event_id) 
    DO UPDATE SET 
        status = EXCLUDED.status,
        error_stack = EXCLUDED.error_stack,
        last_error_message = EXCLUDED.last_error_message,
        http_status_code = EXCLUDED.http_status_code,
        latency_ms = EXCLUDED.latency_ms
    RETURNING id;
    `;

    const values = [
        gateway_id, 
        provider_event_id, 
        event_type, 
        status, 
        http_status, 
        JSON.stringify(payload), 
        error_stack, 
        last_error_message, 
        latency || 0 
    ];

    try {
        const res = await db.query(query, values);
        return res.rows[0];
    } catch (err) {
        console.error("❌ Database Logic Error:", err.message);
        throw err;
    }
};



const getAnalyticsByGateway = async (gateway_id) => {
    const query = `
        SELECT 
            COUNT(*)::int as total_count,
            COUNT(*) FILTER (WHERE status = 'SUCCESS')::int as success_count,
            COUNT(*) FILTER (WHERE status = 'FAILED')::int as failure_count,
            CASE 
                WHEN COUNT(*) > 0 THEN 
                    ROUND((COUNT(*) FILTER (WHERE status = 'SUCCESS')::float / COUNT(*)) * 100)
                ELSE 0 
            END as success_percentage,
            -- Top Errors JSON format mein
            (
                SELECT json_agg(t) FROM (
                    SELECT last_error_message, COUNT(*)::int as occurrence
                    FROM webhook_logs
                    WHERE gateway_id = $1 AND status = 'FAILED' AND last_error_message IS NOT NULL
                    GROUP BY last_error_message
                    ORDER BY occurrence DESC
                    LIMIT 3
                ) t
            ) as error_breakdown
        FROM webhook_logs
        WHERE gateway_id = $1;
    `;

    const res = await db.query(query, [gateway_id]);
    return res.rows[0]; // Ye object return karega jisme total_count, success_count etc honge
};

module.exports = { logWebhook, getAnalyticsByGateway };