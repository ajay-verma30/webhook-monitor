const {Pool} = require('pg');
require('dotenv').config();

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl:{
        rejectUnauthorized: false
    },
    max: 10,                 // maximum 10 connections — Supabase free tier safe
    min: 2,                  // minimum 2 idle connections ready
    idleTimeoutMillis: 30000,    // 30s idle hone ke baad connection close
    connectionTimeoutMillis: 10000, // 10s mein connection na mile toh error
});
pool.on('error', (err) => {
    console.error('❌ Unexpected DB pool error:', err.message);
});

pool.query('SELECT NOW()', (err, res)=>{
    if(err){
        console.log('Supabase connection error', err.stack);
    }
    else{
        console.log('Supabase Connected Successfully at:', res.rows[0].now);
    }
});

module.exports = {
    query:(text, params)=> pool.query(text,params)
};