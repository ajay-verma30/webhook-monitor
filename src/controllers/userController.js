const db = require('../config/db');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const { logWebhook } = require('../models/webhookModels');

const createUser = async (req, res) => {
  let { email, password, full_name } = req.body;

  if (!email || !password || !full_name) {
    return res.status(400).json({ message: "All fields are required" });
  }

  if (!email.includes("@")) {
    return res.status(400).json({ message: "Invalid email" });
  }

  if (password.length < 6) {
    return res.status(400).json({ message: "Password too short" });
  }

  const emailNormalized = email.toLowerCase().trim();
  const name = full_name.trim();

  try {
    const hashPassword = await bcrypt.hash(password, 10);

    await db.query(
      `INSERT INTO users(email, password_hash, full_name) VALUES($1, $2, $3)`,
      [emailNormalized, hashPassword, name]
    );

    res.status(201).json({
      message: "User Created",
      email: emailNormalized
    });

  } catch (err) {
    if (err.code === '23505') {
      return res.status(400).json({ message: "Email already exists" });
    }
    return res.status(500).json({ error: err.message });
  }
};


const loginUser = async(req,res) =>{
    const {email,password} = req.body;
    try{
        if(!email || !password){
            return res.status(400).json({message:"All details are mandatory!"});    
        }
        const emailNormalized = email.toLowerCase().trim();
        const result = await db.query('SELECT id, email, password_hash FROM users WHERE email = $1',[emailNormalized]);

        if(result.rows.length === 0){
            return res.status(400).json({ message: "Invalid credentials" });
        }
        const user = result.rows[0];

        const checkPassword = await bcrypt.compare(password, user.password_hash);

        if(!checkPassword){
            return res.status(400).json({message:"Invalid credentials"});
        }

        const token = jwt.sign({id: user.id, email: user.email}, process.env.JWT_SECRET, {expiresIn:'15m'});
        const refreshToken = jwt.sign(
  { id: user.id },
  process.env.REFRESH_SECRET,
  { expiresIn: '7d' }
);

const tokenHash = crypto
  .createHash('sha256')
  .update(refreshToken)
  .digest('hex');

await db.query(
  `INSERT INTO refresh_tokens(user_id, token_hash, expires_at)
   VALUES($1, $2, NOW() + INTERVAL '7 days')`,
  [user.id, tokenHash]
);

res.cookie('refreshToken', refreshToken, {
    httpOnly: true,
    secure: false,
    sameSite:'lax',
    maxAge: 7 * 24 * 60 * 60 * 1000
});

return res.status(200).json({
  accessToken: token
});
    }
    catch(err){
        return res.status(500).json({ error: err.message });
    }
}

const refreshAccessToken = async (req, res) => {
  const refreshToken = req.cookies.refreshToken;
  if (!refreshToken) return res.status(401).json({ message: "No refresh token" });

  try {
    const decoded = jwt.verify(refreshToken, process.env.REFRESH_SECRET);
    const tokenHash = crypto.createHash('sha256').update(refreshToken).digest('hex');

    const result = await db.query(
      `SELECT * FROM refresh_tokens WHERE token_hash = $1 AND expires_at > NOW()`,
      [tokenHash]
    );

    if (result.rows.length === 0) {
      return res.status(403).json({ message: "Invalid or expired token" });
    }

    const tokenRecord = result.rows[0];

    // ✅ If token was already used but within grace window (5s), return same new token
    if (tokenRecord.used_at) {
      const usedAt = new Date(tokenRecord.used_at);
      const secondsSinceUse = (Date.now() - usedAt.getTime()) / 1000;

      if (secondsSinceUse < 5) {
        // Still within grace window — safe to reuse, just return a fresh token
        // for this user without rotating again
        const user = await db.query('SELECT id, email FROM users WHERE id = $1', [decoded.id]);
        const accessToken = jwt.sign(
          { id: decoded.id, email: user.rows[0].email },
          process.env.JWT_SECRET,
          { expiresIn: '15m' }
        );
        return res.json({ accessToken }); // No rotation, no new cookie needed
      }

      // Outside grace window — this is token reuse after rotation, potential theft
      // Revoke all tokens for this user
      await db.query('DELETE FROM refresh_tokens WHERE user_id = $1', [decoded.id]);
      return res.status(403).json({ message: "Token reuse detected" });
    }

    // ✅ First use — mark as used instead of deleting immediately
    await db.query(
      'UPDATE refresh_tokens SET used_at = NOW() WHERE token_hash = $1',
      [tokenHash]
    );

    const user = await db.query('SELECT id, email FROM users WHERE id = $1', [decoded.id]);

    const newAccessToken = jwt.sign(
      { id: decoded.id, email: user.rows[0].email },
      process.env.JWT_SECRET,
      { expiresIn: '15m' }
    );
    const newRefreshToken = jwt.sign({ id: decoded.id }, process.env.REFRESH_SECRET, { expiresIn: '7d' });
    const newHash = crypto.createHash('sha256').update(newRefreshToken).digest('hex');

    await db.query(
      `INSERT INTO refresh_tokens(user_id, token_hash, expires_at) 
       VALUES($1, $2, NOW() + INTERVAL '7 days')`,
      [decoded.id, newHash]
    );

    // ✅ Schedule old token cleanup after grace window instead of deleting immediately
    setTimeout(async () => {
      await db.query('DELETE FROM refresh_tokens WHERE token_hash = $1', [tokenHash]);
    }, 5000);

    res.cookie('refreshToken', newRefreshToken, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 7 * 24 * 60 * 60 * 1000
    });

    return res.json({ accessToken: newAccessToken });

  } catch (err) {
    return res.status(403).json({ message: "Invalid or expired token" });
  }
};


const logoutUser = async (req, res) => {
  const refreshToken = req.cookies.refreshToken;

  if (refreshToken) {
    const tokenHash = crypto
      .createHash('sha256')
      .update(refreshToken)
      .digest('hex');

    await db.query(
      'DELETE FROM refresh_tokens WHERE token_hash = $1',
      [tokenHash]
    );
  }

  res.clearCookie('refreshToken');

  return res.json({ message: "Logged out" });
};

module.exports = {
    createUser, loginUser, refreshAccessToken, logoutUser
}