const { Telegraf, Markup } = require('telegraf');
//const {MenuTemplate, MenuMiddleware} = require('telegraf-inline-menu');
const util = require('./modules/util');
const bina = require('./modules/binance');
const trading = require('./modules/trading');
const {Pool} = require('pg');
const fs = require('fs');

util.init_logger({
    errorEventName: 'error',
    logDirectory: './logs', // NOTE: folder must exist and be writable...
    fileNamePattern: 'bot-<DATE>.log',
    dateFormat: 'YYYY.MM.DD'
});
bina.init_logger({
    errorEventName: 'error',
    logDirectory: './logs', // NOTE: folder must exist and be writable...
    fileNamePattern: 'bot-<DATE>.log',
    dateFormat: 'YYYY.MM.DD'
});
bina.init_trade_logger({
    errorEventName: 'trade',
    timestampFormat: ' ',
    logDirectory: './logs', // NOTE: folder must exist and be writable...
    fileNamePattern: 'trade-<DATE>.log',
    dateFormat: 'YYYY.MM.DD'
});

const conf = util.includeConfig('../app_conf.json');

util.log('index.js connecting to db...');
const pool = new Pool({
    connectionString: conf.connectionDB,
    max: 2,
    min: 1,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 10000
});

let bot, is_restart=false;

const send_contact_option = {
    "parse_mode": "Markdown",
    "reply_markup": {
        "one_time_keyboard": true,
        "keyboard": [[{
            text: "Отправить мой номер",
            request_contact: true
        }], ["Отмена"]]
    }
}

const help = `List of available commands:
/start - start working with bot
/balances - get USDT balance
/chart_pos - active position charts
/pos - list of all active positions and orders
/state - current trading state
/chart <symbol> YYYY-MM-DD_HH:MM YYYY-MM-DD_HH:MM <account> - show chart for the period
/profit - profit history (n1 by default)
/profitfile <account> <monthes> - profit history in csv-file (n1 by default)

configuration:
/get <account> - show trading settings (n1 by default)
/set <account> <parameter> <value> - set parameter
/calc <account> - show trading calculations
/symbols - show symbol settings
/symbol <account> <symbol>+ - add symbol to the list
/symbol <account> <symbol>- - remove symbol to the list

api:
/apis - show my Binance api keys
/set <account> apikey <apikey> - setup Binance apikey (min 15$)
/set <account> secretkey <secretkey> - setup Binance secretkey
/add <copy from account> - add new Binance account
/del <account> - remove Binance account

/notifications <account> - notification settings (n1 by default)
/help - this help`;

async function registration(msg) {
    util.log('in registration:' + msg.message.from.id + ',' + msg.message.contact.first_name+ ',' + msg.message.contact.phone_number);

    let db_query = {
        name: 'bina.f_reg',
        text: 'SELECT bina.f_reg($1, $2, $3) av_error',
        values: [msg.message.from.id, msg.message.contact.first_name, msg.message.contact.phone_number]
    };
//    msg.message.contact.phone_number / msg.contact.phone_number

    let res;
    try {
        res = await pool.query(db_query);
        //console.log(res.rows[0].av_config);

        return res.rows[0].av_error;
    } catch (err) {
        util.error('error in registration ' + err.message + ' ' + util.safeStringify(err.stack));
        util.log('reg data:' + msg.message.from.id + ',' + msg.message.contact.first_name+ ',' + msg.message.contact.phone_number);
        return 'internal error';
    }
}

async function get_calc(msg) {
    let str = msg.message.text.split(' ');
    let acc_num;
    if (str[1]) {
        acc_num = str[1].substring(1, 10);
    }
    //console.log('in get_calc ' + msg.message.from.id + ' ' + acc_num);

    let db_query = {
        name: 'bina.f_get_calc',
        text: 'SELECT bina.f_get_calc($1, $2) av_calc',
        values: [msg.message.from.id, acc_num]
    };

    let res;
    try {
        res = await pool.query(db_query);
        //console.log(res.rows[0].av_config);

        return res.rows[0].av_calc;
    } catch (err) {
        util.error('error in get_calc ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}

async function get_config(msg) {
    let str = msg.message.text.split(' ');
    let acc_num;
    if (str[1]) {
        acc_num = str[1].substring(1, 10);
    }
    //console.log('in get_config ' + msg.message.from.id + ' ' + acc_num);

    let db_query = {
        name: 'bina.f_get_config',
        text: 'SELECT bina.f_get_config($1, $2) av_config',
        values: [msg.message.from.id, acc_num]
    };

    let res;
    try {
        res = await pool.query(db_query);
        //console.log(res.rows[0].av_config);

        return res.rows[0].av_config;
    } catch (err) {
        util.error('error in get_config ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}

async function set_config(msg) {
    let str = msg.message.text.split(' ');
    let acc_num, parameter, value;
    if (str[1]) {
        acc_num = str[1].substring(1, 10);
    }
    if (str[2]) {
        parameter = str[2];
    }
    if (str[3]) {
        value = str[3];
    }
    //console.log('in set_config ' + msg.message.from.id + ' ' + acc_num);

    let db_query = {
        name: 'bina.f_set_config',
        text: 'SELECT bina.f_set_config($1, $2, $3, $4) av_error',
        values: [msg.message.from.id, acc_num, parameter, value]
    };

    let res;
    try {
        res = await pool.query(db_query);
        //console.log(res.rows[0].av_error);

        return res.rows[0].av_error;
    } catch (err) {
        util.error('error in set_config ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}

async function save_api_validation(subscriber_id, balance_usdt, api_validation_error) {
    let db_query = {
        name: 'bina.f_save_api_validation',
        text: 'SELECT bina.f_save_api_validation($1, $2, $3) av_error',
        values: [subscriber_id, balance_usdt, api_validation_error]
    };

    try {
        let res = await pool.query(db_query);
        return res.rows[0].av_error;
    } catch (err) {
        util.error('error in save_api_validation ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}

async function get_apis(msg) {
    let db_query = {
        name: 'bina.f_get_api',
        text: 'SELECT bina.f_get_api($1) av_api',
        values: [msg.message.from.id]
    };

    let res;
    try {
        res = await pool.query(db_query);
        //console.log(res.rows[0].av_api);

        return res.rows[0].av_api;
    } catch (err) {
        util.error('error in get_apis ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}

async function get_state(msg) {
    let db_query = {
        name: 'bina.f_get_state',
        text: 'SELECT bina.f_get_state($1) av_config',
        values: [msg.message.from.id]
    };

    let res;
    try {
        res = await pool.query(db_query);
        //console.log(res.rows[0].av_config);

        return res.rows[0].av_config;
    } catch (err) {
        util.error('error in get_state ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}

//symbol n1 GMXUSDT+
//symbol n2 GMXUSDT-
async function set_symbol(msg) {
    let acc_num, symbol_length, symbol, operation, db_query, res;
    let str = msg.message.text.split(' ');
    if (str[1]) {
        acc_num = str[1].substring(1, 10);
    }
    if (!acc_num) {
        return 'unknown account';
    }
    if (!str[2]) {
        return 'unknown symbol';
    }

    symbol_length = str[2].length;
    symbol = str[2].substring(0, symbol_length - 1);
    operation = str[2].substring(symbol_length - 1, symbol_length);

    if (operation === '+' || operation === '-') {
        db_query = {
            name: 'bina.f_set_symbol',
            text: 'SELECT bina.f_set_symbol($1, $2, $3, $4) av_error',
            values: [msg.message.from.id, acc_num, symbol, operation]
        };

        try {
            res = await pool.query(db_query);
            return res.rows[0].av_error;
        } catch (err) {
            util.error('error in set_symbol ' + err.message + ' ' + util.safeStringify(err.stack));
            return 'internal error';
        }
    } else {
        return 'unknown operation';
    }
}

async function get_symbols(msg) {
    let acc_num;
    let str = msg.message.text.split(' ');
    if (str[1]) {
        acc_num = str[1].substring(1, 10);
    }

    let db_query = {
        name: 'bina.f_get_symbols',
        text: 'SELECT bina.f_get_symbols($1, $2) av_config',
        values: [msg.message.from.id, acc_num]
    };

    let res;
    try {
        res = await pool.query(db_query);

        return res.rows[0].av_config;
    } catch (err) {
        util.error('error in get_symbols ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}


async function get_notifications(msg) {
    let acc_num;
    let str = msg.message.text.split(' ');
    if (str[1]) {
        acc_num = str[1].substring(1, 10);
    }

    let db_query = {
        name: 'bina.f_get_notifications',
        text: 'SELECT bina.f_get_notifications($1, $2) av_config',
        values: [msg.message.from.id, acc_num]
    };

    let res;
    try {
        res = await pool.query(db_query);

        return res.rows[0].av_config;
    } catch (err) {
        util.error('error in get_notifications ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}

async function get_profit(msg) {
    let acc_num;
    let str = msg.message.text.split(' ');
    if (str[1]) {
        acc_num = str[1].substring(1, 10);
    }

    let db_query = {
        name: 'bina.f_get_profit_history',
        text: 'SELECT bina.f_get_profit_history($1, $2) av_profit',
        values: [msg.message.from.id, acc_num]
    };

    let res;
    try {
        res = await pool.query(db_query);

        return res.rows[0].av_profit;
    } catch (err) {
        util.error('error in get_profit ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}

async function get_profitfile(msg) {
    let acc_num = 1, months = 1;
    let str = msg.message.text.split(' ');
    if (str[1]) {
        acc_num = str[1].substring(1, 10);
    }
    if (str[2]) {
        months = str[2];
    }

    let db_query = {
        name: 'bina.f_get_profit_file',
        text: 'SELECT an_subscriber_id, av_datetime, av_file FROM bina.f_get_profit_file($1, $2, $3)',
        values: [msg.message.from.id, acc_num, months]
    };

    let res;
    try {
        res = await pool.query(db_query);
        let subscriber_id = res.rows[0].an_subscriber_id;
        let datetime = res.rows[0].av_datetime;
        let data = res.rows[0].av_file;
        let filename = 'data_' + subscriber_id + '_' + acc_num + '_' + datetime + '.csv';

        return {filename:filename, data:data};
    } catch (err) {
        util.error('error in get_profitfile ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}


async function add_api(msg) {
    let acc_num;
    let str = msg.message.text.split(' ');
    if (str[1]) {
        acc_num = str[1].substring(1, 10);
    }

    if (!acc_num) {
        return 'Please specify account for copy from, for example: /add n1';
    }
    //console.log('in add_api ' + msg.message.from.id + ' ' + acc_num);

    let db_query = {
        name: 'bina.f_add_api',
        text: 'SELECT bina.f_add_api($1, $2) av_acc_num',
        values: [msg.message.from.id, acc_num]
    };

    let res;
    try {
        res = await pool.query(db_query);
        //console.log(res.rows[0].av_acc_num);

        return 'API added: n' + res.rows[0].av_acc_num;
    } catch (err) {
        util.error('error in add_api ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}

async function del_api(msg) {
    let str = msg.message.text.split(' ');
    let acc_num;
    if (str[1]) {
        acc_num = str[1].substring(1, 10);
    }

    if (!acc_num) {
        return 'Please specify account for remove, for example: /del n1';
    }
    //console.log('in del_api ' + msg.message.from.id + ' ' + acc_num);
    let parameter = 'apiremove';
    let value;

    let db_query = {
        name: 'bina.f_set_config',
        text: 'SELECT bina.f_set_config($1, $2, $3, $4) av_error',
        values: [msg.message.from.id, acc_num, parameter, value]
    };

    let res;
    try {
        res = await pool.query(db_query);
        //console.log(res.rows[0].av_acc_num);

        return 'API added: n' + res.rows[0].av_acc_num;
    } catch (err) {
        util.error('error in del_api ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'internal error';
    }
}

async function get_credentials(msg) {
    let db_query = {
        name: 'bina.f_get_api_credentials',
        text: 'SELECT bina.f_get_api_credentials($1) atj_credentials',
        values: [msg.message.from.id]
    };

    let res;
    try {
        res = await pool.query(db_query);
        //console.log(res.rows[0].atj_credentials);

        return res.rows[0].atj_credentials;
    } catch (err) {
        util.error('error in get_credentials ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function start_bot() {
    bot = new Telegraf(conf.bot.token);
    bot.launch();

    bot.on('message', async (msg) => {
        try {
            /*console.log('=======================');
            console.log(msg.message);
            console.log('=======================');*/

            util.log(msg.message.from.first_name + ' command:' + msg.message.text);

            if (msg.chat.type === 'private' || (msg.message.text && msg.message.text.indexOf('@' + conf.bot.name) >= 0)) {

                if (/\/start$/.test(msg.message.text)) {
                    await msg.telegram.sendMessage(msg.message.chat.id, 'For registration please send us your contact', send_contact_option);

                } else if (/^\/pos/.test(msg.message.text)) {
                    let cred = await get_credentials(msg);
                    let answer = '', now = new Date();
                    if (cred) {
                        for (let i = 0; i < cred.length; i++) {
                            if (cred[i].api_validated === 'Y') {
                                let event = trading.get_trade_event(cred[i].subscriber_id);
                                answer += 'account n' + cred[i].acc_num + '\n--------------------------\n';
                                let positions = await bina.getActivePos(cred[i].binance_acc);
                                if (positions && positions.length > 0) {
                                    for (let pi=0; pi<positions.length; pi++) {
                                        let elem = positions[pi];
                                        if (elem.positionAmt > 0 || elem.positionAmt < 0) {
                                            util.log(cred[i].subscriber_id + '>> ' + elem.side + ' ' + elem.symbol + ': ' + elem.positionAmt + ' entry:' + elem.entryPrice);
                                            elem.unRealizedProfit = bina.trimToDecimalPlaces(elem.unRealizedProfit, 3);
                                            answer += elem.symbol + ': ' + elem.side + ' ' + elem.positionAmt + ' entry:' + elem.entryPrice + ' unRealizedProfit:' + elem.unRealizedProfit + '\n';
                                            if (event.unix_time) {
                                                answer += '  `/chart ' + event.symbol + ' ' + util.format_date_time(event.unix_time - 60 * 1000) + ' ' + util.format_date_time(now + 60 * 1000) + ' n' + event.acc_num + '`\n';
                                            }
                                            answer += '  `/stop n' + cred[i].acc_num + '`\n';
                                        }
                                    }
                                    /*positions.forEach(function (elem, ind) {
                                        if (elem.positionAmt > 0 || elem.positionAmt < 0) {
                                            util.log(positions[ind].symbol + ': ' + positions[ind].positionAmt + ' entry:' + positions[ind].entryPrice);
                                            positions[ind].unRealizedProfit = bina.trimToDecimalPlaces(positions[ind].unRealizedProfit, 3);
                                            answer += positions[ind].symbol + ': ' + positions[ind].positionAmt + ' entry:' + positions[ind].entryPrice + ' unRealizedProfit:' + positions[ind].unRealizedProfit + '\n';
                                            if (event.unix_time) {
                                                answer += '  `/chart ' + event.symbol + ' ' + util.format_date_time(event.unix_time - 60 * 1000) + ' ' + util.format_date_time(now + 60 * 1000) + ' n' + event.acc_num + '`\n';
                                            }
                                            answer += '  `/stop n' + cred[i].acc_num + '`\n';
                                        }
                                    });*/
                                } else {
                                    answer += 'No active positions\n';
                                }

                                answer += await bina.getActiveOrders(cred[i].acc_num, cred[i].binance_acc) + '\n';
                            }
                        }
                    } else {
                        answer += 'no accounts';
                    }
                    answer = util.fixTgMarkup(answer);
                    util.log('pos+' + answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/stop/.test(msg.message.text)) {
                    let str = msg.message.text.split(' ');
                    let acc_num;
                    if (str[1]) {
                        acc_num = str[1].substring(1, 10);
                    }

                    let cred = await get_credentials(msg);
                    let answer = '';
                    if (cred) {
                        for (let i=0; i<cred.length; i++) {
                            //util.log('closeActivePosition ' + i + ' ' + cred[i].acc_num + ' ? ' + acc_num + ' ' + cred[i].subscriber_id);
                            if (cred[i].acc_num === Number(acc_num)) {
                                await trading.stop_trading(cred[i].subscriber_id, cred[i].binance_acc, 'stop');
                                //await bina.closeActivePosition(cred[i].subscriber_id, cred[i].binance_acc,'stop');
                                //await trading.cancelActiveOrders({subscriber_id: cred[i].subscriber_id, binance_acc:cred[i].binance_acc}, 'stop command');
                                answer = 'position closed and all orders cancelled';
                            }
                        }
                        if (!answer) {
                            answer = 'no active positions';
                        }
                    } else {
                        answer = 'no accounts';
                    }
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/cancel/.test(msg.message.text)) {
                    let str = msg.message.text.split(' ');
                    let acc_num, symbol, orderid;
                    if (str[1]) {
                        acc_num = str[1].substring(1, 10);
                    }
                    symbol = str[2];
                    orderid = str[3];
                    let cred = await get_credentials(msg);
                    let answer;
                    if (cred) {
                        for (let i = 0; i < cred.length; i++) {
                            if (cred[i].acc_num === Number(acc_num)) {
                                await bina.cancelOrder(symbol, orderid, cred[i].binance_acc);
                                answer = 'order canceled';
                            }
                        }
                        if (!answer) {
                            answer = 'no active orders';
                        }
                    } else {
                        answer = 'no accounts';
                    }
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/\/balances/.test(msg.message.text)) {
                    let cred = await get_credentials(msg);
                    let answer = '';
                    if (cred) {
                        for (let i = 0; i < cred.length; i++) {
                            if (cred[i].api_validated === 'Y') {
                                answer += 'account n' + cred[i].acc_num + '\n--------------------------\n';
                                answer += await bina.getBalances(cred[i].binance_acc) + '\n';
                            }
                        }
                    } else {
                        answer += 'No accounts';
                    }
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});


                } else if (/^\/calc/.test(msg.message.text)) {
                    let answer = await get_calc(msg);
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/get/.test(msg.message.text)) {
                    let answer = await get_config(msg);
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/apis$/.test(msg.message.text)) {
                    let answer = await get_apis(msg);
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/state$/.test(msg.message.text)) {
                    let answer = await get_state(msg);
                    answer = util.fixTgMarkup(answer);
                    util.log('state+' + answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/add\s/.test(msg.message.text)) {
                    let answer = await add_api(msg);
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/symbol\s/.test(msg.message.text)) {
                    let answer = await set_symbol(msg);
                    await msg.telegram.sendMessage(msg.message.chat.id, 'list modified', {parse_mode: 'Markdown'});

                } else if (/^\/symbols/.test(msg.message.text)) {
                    let answer = await get_symbols(msg);
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/notifications/.test(msg.message.text)) {
                    let answer = await get_notifications(msg);
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/profitfile/.test(msg.message.text)) {
                    let res = await get_profitfile(msg);

                    try {
                        await fs.writeFileSync('./chart/profitfile/' + res.filename, res.data);
                        let answer = conf.http.url + ':' + conf.http.port+ '/profitfile/' + res.filename;
                        answer = util.fixTgMarkup(answer);
                        await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});
                    } catch (err) {
                        util.error('Error in chart writeFileSync ' + err.message + ' ' + util.safeStringify(err.stack));
                        await msg.telegram.sendMessage(msg.message.chat.id, 'no data for this period');
                    }

                } else if (/^\/profit/.test(msg.message.text)) {
                    let answer = await get_profit(msg);
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/del\s/.test(msg.message.text)) {
                    let answer = await del_api(msg);
                    if (!answer) {
                        answer = 'API removed';
                    }
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/set\s/.test(msg.message.text)) {
                    let answer = await set_config(msg);
                    if (!answer) {
                        answer = 'Parameter updated';
                        //answer = 'parameters currently can not be updated!';
                    } else if (answer === 'api_validation_required') {
                        let cred = await get_credentials(msg);
                        let valid = false;
                        if (cred) {
                            for (let i = 0; i < cred.length; i++) {
                                //console.log('b'+i + ' ' + cred[i].subscriber_id + ' ' + cred[i].api_validation_allowed);
                                if (cred[i].api_validation_allowed === 'Y') {
                                    let balance_usdt = await bina.getBalance('USDT', cred[i].binance_acc);
                                    console.log('balance_usdt ' + cred[i].subscriber_id + ' ' + balance_usdt);
                                    answer = await save_api_validation(cred[i].subscriber_id, balance_usdt, '');
                                    valid = true;
                                }
                            }
                        }
                        if (!valid) {
                            answer = 'Too many api revalidations. Try again later';
                        }
                    }
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/chart_pos/.test(msg.message.text)) {
                    let cred = await get_credentials(msg);
                    let answer = '', now = new Date();
                    if (cred) {
                        for (let i = 0; i < cred.length; i++) {
                            if (cred[i].api_validated === 'Y') {
                                let event = trading.get_trade_event(cred[i].subscriber_id);
                                let positions = await bina.getActivePos(cred[i].binance_acc);
                                answer += 'account n' + cred[i].acc_num + '\n--------------------------\n';
                                if (positions && positions.length > 0) {
                                    for (const elem of positions) {
                                        if (elem.positionAmt !== 0 && event.unix_time) {
                                            let from = util.format_date_time(event.unix_time - 60 * 1000);
                                            let to = util.format_date_time(now + 60 * 1000);
                                            let chart_data = await bina.getChart1s(msg.message.chat.id, event.acc_num, event.symbol, from, to);
                                            if (chart_data.aj_chart) {
                                                let tmp_filename = 'chart_' + chart_data.aj_chart.subscriber_id + '_' + event.acc_num + '_' + chart_data.aj_chart.from + '-' + chart_data.aj_chart.to + '_' + event.symbol + '.html';
                                                try {
                                                    await fs.writeFileSync('./chart/' + tmp_filename, chart_data.av_template);
                                                    answer += conf.http.url + ':' + conf.http.port+ '/' + tmp_filename + '\n\n';
                                                } catch (err) {
                                                    util.error('Error in chart writeFileSync ' + err.message + ' ' + util.safeStringify(err.stack));
                                                    answer += 'no data for this period\n\n';
                                                }
                                            } else {
                                                answer += 'no data for this period\n\n';
                                            }
                                        }
                                    }
                                } else {
                                    answer += 'no active positions\n\n';
                                }
                            }
                        }
                    } else {
                        answer += 'No accounts';
                    }

                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});

                } else if (/^\/chart/.test(msg.message.text)) {
                    let str, symbol, from, to, acc_num;
                    str = msg.message.text.split(' ');
                    symbol = str[1]; from = str[2]; to = str[3];
                    if (!symbol) {
                        await msg.telegram.sendMessage(msg.message.chat.id, 'Error!\nPlease specify symbol');
                        return;
                    }
                    if (str[4]) {
                        acc_num = str[4].substring(1, 10);
                    } else {
                        acc_num = '1';
                    }

                    let chart_data = await bina.getChart1s(msg.message.chat.id, acc_num, symbol, from, to);
                    if (chart_data.aj_chart) {
                        let tmp_filename = 'chart_' + chart_data.aj_chart.subscriber_id + '_' + acc_num + '_' + chart_data.aj_chart.from + '-' + chart_data.aj_chart.to + '_' + symbol + '.html';
                        try {
                            await fs.writeFileSync('./chart/' + tmp_filename, chart_data.av_template);
                            let answer = conf.http.url + ':' + conf.http.port+ '/' + tmp_filename;
                            answer = util.fixTgMarkup(answer);
                            await msg.telegram.sendMessage(msg.message.chat.id, answer, {parse_mode: 'Markdown'});
                        } catch (err) {
                            util.error('Error in chart writeFileSync ' + err.message + ' ' + util.safeStringify(err.stack));
                            await msg.telegram.sendMessage(msg.message.chat.id, 'no data for this period');
                        }
                    } else {
                        await msg.telegram.sendMessage(msg.message.chat.id, 'no data for this period');
                    }

                } else if (/^\/restart/.test(msg.message.text)) {
                    let answer = 'restarting...';
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer);
                    is_restart = true;

                } else if (/^\/help/.test(msg.message.text)) {
                    let answer = help;
                    //answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer);

                } else if (msg.message.contact && msg.message.contact.phone_number) {
                    let answer = await registration(msg);
                    if (!answer) {
                        answer = 'Registration completed';
                    }
                    answer = util.fixTgMarkup(answer);
                    await msg.telegram.sendMessage(msg.message.chat.id, answer, {"parse_mode": "Markdown", "reply_markup": {remove_keyboard: true}});

                } else {
                    await msg.telegram.sendMessage(msg.message.chat.id, 'Unknown command. Use /help', {parse_mode: 'Markdown'});
                    util.log('unknown command from ' + msg.message.from.first_name + ': ' + msg.message.text);
                }
            }

        } catch(err) {
            util.log('bot on message failed ' + err.message + ' ' + util.safeStringify(err.stack));
        }

    })

}

let core_client_id = 0, rate_loading_started = false;
/*async function rate_loader1s() {
    //console.log(util.getFormattedTime() + ': rate_loader ' + rate_loading_started);
    //util.log(util.getFormattedTime() + ': rate_loader ' + rate_loading_started);

    if (!rate_loading_started) {
        rate_loading_started = true;
        core_client_id++;
        if (core_client_id >= conf.binance_core_accounts.length) {
            core_client_id = 0
        }
        await bina.saveRates1s(core_client_id);
        if (conf.trading) {
            await trading.process1s();
        }
        rate_loading_started = false;
    }
}*/

async function auto_restart() {
    let today = new Date();
    let h = today.getHours().toString().padStart(2, '0');
    let m = today.getMinutes().toString().padStart(2, '0');
    if (h + ":" + m === '09:00') {
        util.log('time for auto restarting........');
        let count_active_trades = trading.get_count_active_trades();
        if (count_active_trades > 0) {
            util.log('restarting will be next time because of existing active trades:' + count_active_trades);
        } else {
            util.log('restarting will be in 10 seconds........');
            is_restart = true;
        }
    }
}

async function check_restart() {
    if (is_restart) {
        await trading.save_trade_state_all();

        util.log('auto restarting right now........');
        await util.sleep(1000);
        await process.exit(1);
        await util.sleep(100);
        await process.abort();
    }
}

async function init() {
    util.log(conf.bot.name + ' started $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$');
    await start_bot();
    trading.set_bot(bot);

    await trading.load_trade_state();

    if (conf.price_loader) {
        //setInterval(rate_loader1s, 1 * 1000); // each second
        //await trading.ws_prices_init(); // web-socket version
        await trading.ws_prices_init_v2(); // web-socket candles version
        setInterval(trading.ws_prices_init_v2, 30 * 60 * 1000); // each 30 minutes
        setInterval(trading.health_check, 30 * 1000); // each 30 seconds
    }

    if (conf.trading) {
        await trading.warming_all();
        await trading.warming_ws_all();
        setInterval(trading.process_trade_events, 30 * 1000); // each 30 seconds
        setInterval(trading.warming_all, 5.5 * 60 * 1000); // each 20 minutes
    }

    setInterval(auto_restart, 60 * 1000); // each 60 seconds
    setInterval(check_restart, 10 * 1000); // each 10 seconds

    await trading.apply_trade_state_after_load();
}

init();

process.on('uncaughtException', function (err) {
    util.log('uncaughtException:' + err.message + ' ' + util.safeStringify(err.stack));
    //process.exit(1);
})


// Enable graceful stop
//process.once('SIGINT', () => bot.stop('SIGINT'));
//process.once('SIGTERM', () => bot.stop('SIGTERM'));
