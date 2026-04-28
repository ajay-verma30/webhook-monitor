const db = require('../config/db');

// 🔥 GET NOTIFICATION CONFIG
const getNotificationConfig = async (req, res) => {
  const { gateway_id } = req.params;
  const userId = req.user.id;

  try {
    const result = await db.query(
      `SELECT * FROM notification_configs 
       WHERE gateway_id = $1 AND user_id = $2`,
      [gateway_id, userId]
    );

    if (result.rows.length === 0) {
      // Default values agar config exist nahi karti
      return res.status(200).json({
        gateway_id,
        slack_enabled: false,
        email_enabled: true,
        amount_threshold: 500,
        failure_threshold: 10,
        notify_threshold_breach: true,
        notify_retry_exhausted: true,
        notify_loss_recovered: true
      });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("GET NOTIF CONFIG ERROR:", err);
    return res.status(500).json({
      error: "Failed to fetch notification configuration"
    });
  }
};

// 🔥 SAVE OR UPDATE NOTIFICATION CONFIG (UPSERT)
const saveNotificationConfig = async (req, res) => {
  const { gateway_id } = req.params;
  const userId = req.user.id;
  const {
    slack_webhook_url,
    slack_enabled,
    amount_threshold,
    failure_threshold,
    notify_threshold_breach,
    notify_retry_exhausted,
    notify_loss_recovered,
    discord_webhook_url,
    discord_enabled,
    teams_webhook_url,
    teams_enabled
  } = req.body;

  try {
    // UPSERT logic: Insert if new, Update if gateway_id exists
    const query = `
      INSERT INTO notification_configs (
        gateway_id, user_id, slack_webhook_url, slack_enabled, 
        amount_threshold, failure_threshold, notify_threshold_breach, 
        notify_retry_exhausted, notify_loss_recovered, 
        discord_webhook_url, discord_enabled, 
        teams_webhook_url, teams_enabled, updated_at
      ) 
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
      ON CONFLICT (gateway_id) 
      DO UPDATE SET 
        slack_webhook_url = EXCLUDED.slack_webhook_url,
        slack_enabled = EXCLUDED.slack_enabled,
        amount_threshold = EXCLUDED.amount_threshold,
        failure_threshold = EXCLUDED.failure_threshold,
        notify_threshold_breach = EXCLUDED.notify_threshold_breach,
        notify_retry_exhausted = EXCLUDED.notify_retry_exhausted,
        notify_loss_recovered = EXCLUDED.notify_loss_recovered,
        discord_webhook_url = EXCLUDED.discord_webhook_url,
        discord_enabled = EXCLUDED.discord_enabled,
        teams_webhook_url = EXCLUDED.teams_webhook_url,
        teams_enabled = EXCLUDED.teams_enabled,
        updated_at = NOW()
      RETURNING *;
    `;

    const values = [
      gateway_id, userId, slack_webhook_url, slack_enabled,
      amount_threshold, failure_threshold, notify_threshold_breach,
      notify_retry_exhausted, notify_loss_recovered,
      discord_webhook_url, discord_enabled,
      teams_webhook_url, teams_enabled
    ];

    const result = await db.query(query, values);

    return res.status(200).json({
      message: "Notification settings saved successfully",
      config: result.rows[0]
    });

  } catch (err) {
    console.error("SAVE NOTIF CONFIG ERROR:", err);
    return res.status(500).json({
      error: err.message || "Failed to save notification settings"
    });
  }
};

module.exports = { getNotificationConfig, saveNotificationConfig };