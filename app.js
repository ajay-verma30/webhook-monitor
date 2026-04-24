const express = require('express');
const dotenv = require('dotenv');
const http = require('http'); // 1. Built-in HTTP module chahiye
const { Server } = require('socket.io'); // 2. Socket.io import
const db = require('./src/config/db'); 
const { registerGateway, getGateways } = require('./src/controllers/adminController');
const cors = require('cors');
const cookieParser = require('cookie-parser');

const { verifyStripeSignature } = require('./src/middlewares/verifyStripe');
const { handleStripeWebhook, getGatewayAnalytics, getWebhookDetails, replayWebhook } = require('./src/controllers/webhookController');
const { createUser, loginUser, refreshAccessToken, logoutUser} = require('./src/controllers/userController');
const authToken  = require('./src/middlewares/authToken');

dotenv.config();
const app = express();


const server = http.createServer(app);

// --- 4. CONFIGURE SOCKET.IO ---
const io = new Server(server, {
    cors: {
        origin: "http://localhost:5173", 
        methods: ["GET", "POST"],
        credentials: true
    }
});

// Socket connection listener
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
  origin: 'http://localhost:5173',
  credentials: true
}));

app.use(cookieParser());

//users
app.post('/api/v1/users/new', createUser);
app.post('/api/v1/users/login', loginUser);
app.post('/api/v1/users/refresh-token', refreshAccessToken );
app.post('/api/v1/users/logout', logoutUser);


app.post('/api/v1/admin/register-gateway', authToken, registerGateway);
app.get('/api/v1/gateways', authToken, getGateways);

app.post(
    '/api/v1/webhooks/:gateway_id',
    verifyStripeSignature, 
    handleStripeWebhook 
);

app.get('/api/v1/analytics/:gateway_id', authToken, getGatewayAnalytics);
app.get('/api/v1/analytics/log/:webhook_id', authToken, getWebhookDetails);
app.post('/api/v1/webhooks/replay/:webhook_id', authToken, replayWebhook);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 HookPulse Backend is Running on port ${PORT}`);
    console.log(`⚡ Socket.io is ready for live updates!`);
});