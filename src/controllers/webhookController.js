const { logWebhook, getAnalyticsByGateway } = require('../models/webhookModels');
const db =require('../config/db');
const axios = require("axios");


const rcaAdvisor = {
    // Decline codes
    'generic_decline': 'Bank-side security block. Suggest customer to contact their bank or try international-enabled card.',
    'insufficient_funds': 'Customer balance is low. Suggest top-up or another card.',
    'expired_card': 'Card is expired. Ask customer to update card details.',
    'fraudulent': 'Transaction flagged as high risk. Do not retry to avoid chargebacks.',
    'incorrect_cvv': 'Manual entry error. Ask customer to re-enter CVV carefully.',
    'do_not_honor': 'Bank rejected without reason. Customer should contact their bank.',
    'card_not_supported': 'Card type not supported by this gateway. Try a different card.',

    'card_declined': 'Card was declined by the issuing bank.',
    'expired_card': 'Card expired. Customer must update payment method.',
    'incorrect_number': 'Invalid card number entered.',
    'processing_error': 'Temporary gateway processing issue. Safe to retry once.',
    'rate_limit': 'Too many requests to gateway. Retry after a short delay.',
};

const handleStripeWebhook = async (req, res) => {
    const io = req.app.get('socketio'); 
    const { gateway_id } = req.params;
    const event = req.stripeEvent;
    const startTime = Date.now();

    let status = "SUCCESS";
    let errorMessage = null;
    let errorStack = null;

    try {
        if (event.type.includes('payment_intent.payment_failed') || event.type.includes('charge.failed')) {
            status = 'FAILED';
            
            const errorData = event.data.object.last_payment_error || event.data.object.error;

            // 🛠️ FIX: RCA pattern matching ke liye codes zyada reliable hote hain
            const d_code = errorData?.decline_code || null; 
            const e_code = errorData?.code || null;         
            const raw_msg = errorData?.message || 'Transaction Declined';
            errorMessage = d_code || e_code || raw_msg;

            errorStack = JSON.stringify({
                code: e_code,
                decline_code: d_code || 'N/A',
                raw_message: raw_msg,
                readable_error: raw_msg 
            });

        } else if (event.type.includes('dispute')) {
            status = 'CRITICAL';
            errorMessage = "dispute_initiated"; // Pattern for RCA
        }

        const latency = Date.now() - startTime;

        // DB Logging
        await logWebhook({
            gateway_id: gateway_id,
            provider_event_id: event.id, 
            event_type: event.type,      
            status: status,
            http_status: 200,
            payload: event,
            error_stack: errorStack,
            last_error_message: errorMessage, // Now saves 'generic_decline'
            latency: latency 
        });

        if (io) {
            io.emit(`update_dashboard_${gateway_id}`, { 
                event: event.type, 
                status: status,
                timestamp: new Date()
            });
        }

        res.status(200).send({ received: true });

    } catch (err) {
        console.error("🔥 Smart Controller Breakdown:", err.message);
        const finalLatency = Date.now() - startTime;
        
        await logWebhook({
            gateway_id: gateway_id,
            provider_event_id: event?.id || 'unknown',
            event_type: event?.type || 'unknown',
            status: 'FAILED',
            http_status: 500,
            payload: event || req.body,
            error_stack: `System Error: ${err.message}`,
            latency: finalLatency
        });
        
        res.status(500).json({ error: 'RCA Processing Failed' });
    }
};

const getGatewayAnalytics = async (req, res) => {
    const { gateway_id } = req.params;
    const { filter = '24h' } = req.query;

    // Mapping filters to PostgreSQL intervals
    const intervals = {
        '1h':  { gap: '1 minute', range: '1 hour',   trunc: 'minute', format: 'HH24:MI' },
        '24h': { gap: '1 hour',   range: '24 hours',  trunc: 'hour',   format: 'HH24:00' },
        '7d':  { gap: '1 day',    range: '7 days',    trunc: 'day',    format: 'DD Mon' },
        '30d': { gap: '1 day',    range: '30 days',   trunc: 'day',    format: 'DD Mon' },
        '1y':  { gap: '1 month',  range: '1 year',    trunc: 'month',  format: 'Mon YYYY' }
    };

    const config = intervals[filter] || intervals['24h'];

    try {
        // 1. Time-Filtered Summary Stats
        // Hum model call karne ki jagah yahan direct query likh rahe hain taaki 'filter' apply ho sake
        const summaryQuery = `
            SELECT 
                COUNT(*) as total_count,
                COUNT(*) FILTER (WHERE status = 'SUCCESS') as success_count,
                COUNT(*) FILTER (WHERE status IN ('FAILED', 'CRITICAL')) as failure_count
            FROM webhook_logs 
            WHERE gateway_id = $1 
            AND received_at >= NOW() - INTERVAL '${config.range}';
        `;

        // 2. Time-Filtered Chart Data
        const chartQuery = `
            SELECT 
                to_char(series.time_slot, '${config.format}') AS label,
                COALESCE(COUNT(w.id) FILTER (WHERE w.status = 'SUCCESS'), 0) AS success,
                COALESCE(COUNT(w.id) FILTER (WHERE w.status IN ('FAILED', 'CRITICAL')), 0) AS failed
            FROM 
                generate_series(
                    date_trunc('${config.trunc}', NOW() - INTERVAL '${config.range}') + INTERVAL '${config.gap}', 
                    date_trunc('${config.trunc}', NOW()), 
                    '${config.gap}'::interval
                ) AS series(time_slot)
            LEFT JOIN webhook_logs w ON date_trunc('${config.trunc}', w.received_at) = series.time_slot 
                AND w.gateway_id = $1
            GROUP BY series.time_slot
            ORDER BY series.time_slot ASC;
        `;

        // 3. Time-Filtered Top Errors
        const errorsQuery = `
            SELECT last_error_message, COUNT(*) as occurrence
            FROM webhook_logs
            WHERE gateway_id = $1 
            AND status IN ('FAILED', 'CRITICAL')
            AND received_at >= NOW() - INTERVAL '${config.range}'
            GROUP BY last_error_message
            ORDER BY occurrence DESC
            LIMIT 5;
        `;

// 3. Time-Filtered Recent Hits (Table Data)
const recentHitsQuery = `
    SELECT 
        id,
        event_type, 
        status, 
        to_char(received_at, 'HH24:MI:SS') as time
    FROM webhook_logs
    WHERE gateway_id = $1
    AND event_type IN (
        'payment_intent.succeeded', 
        'payment_intent.payment_failed', 
        'charge.failed', 
        'charge.refunded',
        'customer.subscription.deleted',
        'customer.subscription.created'
    )
    ORDER BY received_at DESC
    LIMIT 10;
`;

        const [summaryRes, chartRes, errorsRes, recentHitsRes] = await Promise.all([
            db.query(summaryQuery, [gateway_id]),
            db.query(chartQuery, [gateway_id]),
            db.query(errorsQuery, [gateway_id]),
            db.query(recentHitsQuery, [gateway_id])
        ]);

        const stats = summaryRes.rows[0];
        const total = parseInt(stats.total_count);
        const success = parseInt(stats.success_count);
        const failure = parseInt(stats.failure_count);
        
        const successRate = total > 0 ? Math.round((success / total) * 100) : 0;

        res.status(200).json({
            gateway_id,
            summary: {
                total_events: total,
                success_rate: `${successRate}%`,
                failed_events: failure,
                success_events: success
            },
            top_errors: errorsRes.rows,
            chart_data: chartRes.rows,
            recent_hits: recentHitsRes.rows
        });

    } catch (err) {
        console.error("❌ Analytics API Error:", err.message);
        res.status(500).json({ error: `Analytics failed: ${err.message}` });
    }
};


// Controller: getWebhookDetails
const getWebhookDetails = async (req, res) => {
    const { webhook_id } = req.params;
    try {
        const query = `
            SELECT 
    w.*, 
    r.issue_category, 
    r.suggested_fix, 
    r.severity AS rca_severity
FROM webhook_logs w
LEFT JOIN LATERAL (
    SELECT *
    FROM rca_rules r
    WHERE 
        (
          r.decline_code IS NOT NULL 
          AND COALESCE(
                w.payload #>> '{data,object,last_payment_error,decline_code}',
                w.payload #>> '{data,object,outcome,reason}'
              ) = r.decline_code
        )
        OR
        (
          r.error_code IS NOT NULL 
          AND COALESCE(
                w.payload #>> '{data,object,last_payment_error,code}',
                w.payload #>> '{data,object,failure_code}'
              ) = r.error_code
        )
        OR
        (
          r.error_pattern IS NOT NULL 
          AND w.last_error_message ILIKE '%' || r.error_pattern || '%'
        )
    ORDER BY 
        CASE 
            WHEN r.decline_code IS NOT NULL 
                 AND COALESCE(
                      w.payload #>> '{data,object,last_payment_error,decline_code}',
                      w.payload #>> '{data,object,outcome,reason}'
                 ) = r.decline_code 
            THEN 3
            
            WHEN r.error_code IS NOT NULL 
                 AND COALESCE(
                      w.payload #>> '{data,object,last_payment_error,code}',
                      w.payload #>> '{data,object,failure_code}'
                 ) = r.error_code 
            THEN 2
            
            WHEN r.error_pattern IS NOT NULL 
                 AND w.last_error_message ILIKE '%' || r.error_pattern || '%'
            THEN 1
            
            ELSE 0
        END DESC,
        r.id DESC
    LIMIT 1
) r ON true
WHERE w.id = $1;
        `;
        
        const result = await db.query(query, [webhook_id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Webhook not found" });
        }

        res.status(200).json(result.rows[0]);
    } catch (err) {
        console.error("❌ RCA Engine Error:", err.message);
        res.status(500).json({ error: "Failed to run RCA analysis" });
    }
};


const replayWebhook = async (req, res) => {
    const { id } = req.params;

    try {
        // 1. Get original webhook
        const result = await db.query(
            `SELECT wl.*, g.webhook_secret 
             FROM webhook_logs wl
             JOIN gateways g ON wl.gateway_id = g.id
             WHERE wl.id = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Webhook not found" });
        }

        const webhook = result.rows[0];

    const targetUrl = webhook.webhook_url; 

        const start = Date.now();

        let response;
        let status = "SUCCESS";
        let errorStack = null;

        try {
            response = await axios.post(targetUrl, webhook.payload, {
                headers: webhook.request_headers || {},
                timeout: 5000
            });
        } catch (err) {
            status = "FAILED";
            errorStack = err.message;
        }

        const latency = Date.now() - start;

        await db.query(
  `INSERT INTO webhook_logs (
    gateway_id,
    provider_event_id,
    event_type,
    status,
    http_status_code,
    latency_ms,
    payload,
    request_headers,
    error_stack,
    last_error_message,
    retry_count,
    replayed_from
  )
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
  [
    webhook.gateway_id,
    webhook.provider_event_id + "_replay",
    webhook.event_type,
    status,
    response?.status || 500,
    latency,
    webhook.payload,
    webhook.request_headers,
    errorStack,
    errorStack,
    webhook.retry_count + 1,
    webhook.id // 👈 THIS IS NEW
  ]
);

        return res.json({
            message: "Webhook replayed",
            status
        });

    } catch (err) {
        console.error("Replay error:", err);
        res.status(500).json({ error: "Replay failed" });
    }
};
module.exports = { handleStripeWebhook, getGatewayAnalytics, getWebhookDetails, replayWebhook };