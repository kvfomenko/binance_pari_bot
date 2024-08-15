"use strict";
const util = require('./modules/util');
const bina = require('./modules/binance');
const {Pool} = require('pg');

const conf = util.includeConfig('../app_conf.json');

util.init_logger({
    errorEventName: 'error',
    logDirectory: './logs', // NOTE: folder must exist and be writable...
    fileNamePattern: 'jobs-<DATE>.log',
    dateFormat: 'YYYY.MM.DD'
});
bina.init_logger({
    errorEventName: 'error',
    logDirectory: './logs', // NOTE: folder must exist and be writable...
    fileNamePattern: 'jobs-<DATE>.log',
    dateFormat: 'YYYY.MM.DD'
});

let stat_logger;
stat_logger = require('simple-node-logger').createRollingFileLogger({
    errorEventName: 'rate_stat',
    timestampFormat: ' ',
    logDirectory: './logs', // NOTE: folder must exist and be writable...
    fileNamePattern: 'rate_stat-<DATE>.log',
    dateFormat: 'YYYY.MM.DD'
});

util.log('jobs.js connecting to db...');
const pool = new Pool({
    connectionString: conf.connectionDB,
    max: 2,
    min: 1,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 10000
});


async function build_rate_stat() {
    util.log('build_rate_stat');
    try {
        let db_query = {
            name: 'bina.f_build_rate_stat',
            text: 'SELECT bina.f_build_rate_stat() as av_backup_sql',
            values: []
        };
        let res = await pool.query(db_query);

        let backup_sql = res.rows[0].av_backup_sql;
        if (backup_sql && backup_sql.length > 0) {
            for (let i=0; i<backup_sql.length; i++) {
                stat_logger.info(backup_sql[i]);
            }
        }

        //return res.rows[0].av_backup_sql;
    } catch(err) {
        util.error('build_rate_stat failed ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function cleanup() {
    util.log('cleanup');
    try {
        let db_query = {
            name: 'bina.f_cleanup',
            text: 'SELECT bina.f_cleanup()',
            values: []
        };
        let res = await pool.query(db_query);
    } catch(err) {
        util.error('cleanup failed ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function exchange_loader() {
    util.log('exchange_loader');
    try {
        await bina.saveExchangeInfo();
    } catch(err) {
        util.error('exchange_loader failed ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function get_state_list() {
    let db_query = {
        name: 'bina.f_get_state_list',
        text: 'SELECT bina.f_get_state_list() aj_subscribers',
        values: []
    };

    let res;
    try {
        res = await pool.query(db_query);
        return res.rows[0].aj_subscribers;
    } catch (err) {
        util.error('error in get_state_list ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function actualize_state_all() {
    util.log('actualize_state_all...');
    let list = await get_state_list();

    if (list && list.length) {
        for (let i = 0; i<list.length; i++) {
            await bina.actualize_state(list[i]);
        }
    }
}

async function init() {
    util.log('jobs starting $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$');
//    await actualize_state_all();
    build_rate_stat();
    setInterval(build_rate_stat, 10.5 * 60 * 1000); // each 10 minutes
    setInterval(cleanup, 61.3 * 60 * 1000); // each 1 hour
//    setInterval(actualize_state_all, 6 * 60 * 1000); // each 5 minutes
    setInterval(exchange_loader, 60.2 * 60 * 1000); // each 1 hours
}

init();

process.on('uncaughtException', function (err) {
    util.log('uncaughtException:' + err.message);
    util.log(err.stack);
    process.exit(1);
})


// Enable graceful stop
/*process.once('SIGINT', () => bot.stop('SIGINT'));
process.once('SIGTERM', () => bot.stop('SIGTERM'));*/
