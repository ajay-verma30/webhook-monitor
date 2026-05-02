const express = require("express");
const dotenv = require("dotenv");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const cookieParser = require("cookie-parser");
const promClient = require("prom-client");

const db = require("./src/config/db");
const PROVIDERS = require("./src/providers");

require("./src/queues/retryQueue");
const { getMetrics } = require("./src/queues/retryQueue");

// Controllers
const {
  registerGateway,
  getGateways,
} = require("./src/controllers/adminController");
const {
  getGatewayAnalytics,
  getWebhookDetails,
  replayWebhook,
} = require("./src/controllers/webhookController");

const {
  createUser,
  loginUser,
  refreshAccessToken,
  logoutUser,
} = require("./src/controllers/userController");

const {
  getNotificationConfig,
  saveNotificationConfig,
} = require("./src/controllers/notificationRoutes");

const authToken = require("./src/middlewares/authToken");

dotenv.config();

const app = express();
const server = http.createServer(app);

// ───────────────────────── SOCKET ─────────────────────────
const io = new Server(server, {
  cors: {
    origin: ["http://localhost:5173", "http://localhost:3000"],
    methods: ["GET", "POST"],
    credentials: true,
  },
});

io.on("connection", (socket) => {
  console.log(`🔌 Dashboard Connected: ${socket.id}`);
  socket.on("disconnect", () => {
    console.log("❌ Dashboard Disconnected");
  });
});

app.set("socketio", io);

// ───────────────────────── RAW BODY (IMPORTANT FOR STRIPE) ─────────────────────────
app.use(
  express.json({
    verify: (req, res, buf) => {
      req.rawBody = buf;
    },
  }),
);

app.use(
  cors({
    origin: ["http://localhost:5173", "http://localhost:3000"],
    credentials: true,
  }),
);

app.use(cookieParser());

// ───────────────────────── USER ROUTES ─────────────────────────
app.post("/api/v1/users/new", createUser);
app.post("/api/v1/users/login", loginUser);
app.post("/api/v1/users/refresh-token", refreshAccessToken);
app.post("/api/v1/users/logout", logoutUser);

// ───────────────────────── ADMIN ROUTES ─────────────────────────
app.post("/api/v1/admin/register-gateway", authToken, registerGateway);
app.get("/api/v1/gateways", authToken, getGateways);

// ───────────────────────── DYNAMIC WEBHOOK ENGINE ─────────────────────────
app.post("/api/v1/webhooks/:provider/:gateway_id", async (req, res) => {
  try {
    const { provider, gateway_id } = req.params;
    const providerKey = provider.toLowerCase();

    const providerConfig = PROVIDERS[providerKey];
    if (!providerConfig) {
      return res.status(400).json({ error: "Unsupported provider" });
    }

    const { rows } = await db.query(
      `SELECT LOWER(provider_type::TEXT) AS provider_type
             FROM gateways
             WHERE id = $1`,
      [gateway_id],
    );

    if (!rows.length) {
      return res.status(404).json({ error: "Gateway not found" });
    }

    if (rows[0].provider_type !== providerKey) {
      return res.status(400).json({ error: "Provider mismatch" });
    }

    req.provider = providerKey;
    req.gateway_id = gateway_id;

    await providerConfig.verify(req);
    await providerConfig.handler(req, res);
  } catch (err) {
    console.error("Webhook Error:", err.message);
    return res.status(400).json({ error: err.message });
  }
});

// IMPORTANT: /analytics/logs/:webhook_id must be registered BEFORE
// /analytics/:provider/:gateway_id — Express matches routes top-down,
// so the fixed-segment route must come first to avoid "logs" being
// swallowed as the :provider wildcard and hitting the wrong handler.
// Renamed from /log/ to /logs/ to make the fixed segment clearly distinct.
app.get("/api/v1/analytics/logs/:webhook_id", authToken, getWebhookDetails);
app.get(
  "/api/v1/analytics/:provider/:gateway_id",
  authToken,
  getGatewayAnalytics,
);

// ───────────────────────── REPLAY ─────────────────────────
app.post("/api/v1/webhooks/replay/:webhook_id", authToken, replayWebhook);

// ───────────────────────── TEST ─────────────────────────
app.post("/api/v1/test-destination", (req, res) => {
  res.status(200).json({ received: true });
});

// ───────────────────────── NOTIFICATIONS ─────────────────────────
app.post(
  "/api/v1/notifications/config/:gateway_id",
  authToken,
  saveNotificationConfig,
);
app.get("/api/v1/notifications/config", authToken, getNotificationConfig);

// ───────────────────────── METRICS ─────────────────────────
app.get("/metrics", async (req, res) => {
  res.set("Content-Type", promClient.register.contentType);
  res.send(await getMetrics());
});

// ───────────────────────── START SERVER ─────────────────────────
const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log(`🚀 HookPulse running on port ${PORT}`);
  console.log(`⚡ Socket.io enabled`);
});
