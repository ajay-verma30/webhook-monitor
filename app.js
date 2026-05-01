const express = require('express');
const dotenv = require('dotenv');
const http = require('http');
const { Server } = require('socket.io');
const db = require('./src/config/db'); 
const { registerGateway, getGateways } = require('./src/controllers/adminController');
const cors = require('cors');
const cookieParser = require('cookie-parser');
require('./src/queues/retryQueue')
const { getMetrics } = require('./src/queues/retryQueue');
const promClient = require('prom-client');

const { verifyStripeSignature }  = require('./src/middlewares/verifyStripe');
const { verifyAdyenSignature }   = require('./src/middlewares/verifyAdyen');
const { verifyPaypalSignature }  = require('./src/middlewares/verifyPaypal');

const { handleStripeWebhook, getGatewayAnalytics, getWebhookDetails, replayWebhook } = require('./src/controllers/webhookController');
const { handleAdyenWebhook,  getGatewayAnalytics: getAdyenAnalytics,  getWebhookDetails: getAdyenWebhookDetails,  replayWebhook: replayAdyenWebhook  } = require('./src/controllers/adyenController');
const { handlePaypalWebhook, getGatewayAnalytics: getPaypalAnalytics, getWebhookDetails: getPaypalWebhookDetails, replayWebhook: replayPaypalWebhook } = require('./src/controllers/paypalController');

const { createUser, loginUser, refreshAccessToken, logoutUser } = require('./src/controllers/userController');
const { getNotificationConfig, saveNotificationConfig } = require('./src/controllers/notificationRoutes')
const authToken = require('./src/middlewares/authToken');


dotenv.config();
const app = express();

const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "http://localhost:5173", 
        methods: ["GET", "POST"],
        credentials: true
    }
});

io.on('connection', (socket) => {
    console.log(`🔌 Dashboard Connected: ${socket.id}`);
    socket.on('disconnect', () => {
        console.log('❌ Dashboard Disconnected');
    });
});

app.set('socketio', io);

app.use(express.json({
    verify: (req, res, buf) => {
        req.rawBody = buf; 
    }
}));

app.use(cors({
    origin: [
        'http://localhost:5173',
        'http://localhost:3000'
    ],
    credentials: true
}));

app.use(cookieParser());

// ─── Users ────────────────────────────────────────────────────────────────────
app.post('/api/v1/users/new',           createUser);
app.post('/api/v1/users/login',         loginUser);
app.post('/api/v1/users/refresh-token', refreshAccessToken);
app.post('/api/v1/users/logout',        logoutUser);

// ─── Admin ────────────────────────────────────────────────────────────────────
app.post('/api/v1/admin/register-gateway', authToken, registerGateway);
app.get('/api/v1/gateways',                authToken, getGateways);

// ─── Webhook ingestion (no authToken — called by payment providers) ───────────
app.post('/api/v1/webhooks/stripe/:gateway_id',  verifyStripeSignature,  handleStripeWebhook);
app.post('/api/v1/webhooks/adyen/:gateway_id',   verifyAdyenSignature,   handleAdyenWebhook);
app.post('/api/v1/webhooks/paypal/:gateway_id',  verifyPaypalSignature,  handlePaypalWebhook);

// ─── Analytics ────────────────────────────────────────────────────────────────
app.get('/api/v1/analytics/stripe/:gateway_id',  authToken, getGatewayAnalytics);
app.get('/api/v1/analytics/adyen/:gateway_id',   authToken, getAdyenAnalytics);
app.get('/api/v1/analytics/paypal/:gateway_id',  authToken, getPaypalAnalytics);

// ─── Webhook detail + RCA ─────────────────────────────────────────────────────
app.get('/api/v1/analytics/stripe/log/:webhook_id', authToken, getWebhookDetails);
app.get('/api/v1/analytics/adyen/log/:webhook_id',  authToken, getAdyenWebhookDetails);
app.get('/api/v1/analytics/paypal/log/:webhook_id', authToken, getPaypalWebhookDetails);

// ─── Replay ───────────────────────────────────────────────────────────────────
app.post('/api/v1/webhooks/stripe/replay/:webhook_id', authToken, replayWebhook);
app.post('/api/v1/webhooks/adyen/replay/:webhook_id',  authToken, replayAdyenWebhook);
app.post('/api/v1/webhooks/paypal/replay/:webhook_id', authToken, replayPaypalWebhook);

// ─── Notifications ────────────────────────────────────────────────────────────
app.post('/api/v1/notifications/config/:gateway_id', authToken, saveNotificationConfig);
app.get('/api/v1/notifications/config',              authToken, getNotificationConfig);

// ─── Metrics ──────────────────────────────────────────────────────────────────
app.get('/metrics', async (req, res) => {
    res.set('Content-Type', promClient.register.contentType);
    res.send(await getMetrics());
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 HookPulse Backend is Running on port ${PORT}`);
    console.log(`⚡ Socket.io is ready for live updates!`);
});