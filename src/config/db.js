const {Pool} = require('pg');
require('dotenv').config();

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl:{
        rejectUnauthorized: false
    }
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