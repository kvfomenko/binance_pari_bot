const bina = require('./binance');
const util = require('./util');
const {Pool} = require('pg');
const {WebsocketClient, DefaultLogger} = require("binance");
const Binance = require("node-binance-api");
const MD5 = require("crypto-js/md5");

const conf = util.includeConfig('../app_conf.json');

util.init_logger({
    errorEventName: 'error',
    logDirectory: './logs', // NOTE: folder must exist and be writable...
    fileNamePattern: 'bot-<DATE>.log',
    dateFormat: 'YYYY.MM.DD'
});

util.log('trading.js connecting to db...');
const pool = new Pool({
    connectionString: conf.connectionDB,
    max: 5,
    min: 1,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 3000
});
const poole = new Pool({
    connectionString: conf.connectionDB,
    max: 3,
    min: 1,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 3000
});

let bot;
let trade_state = [];
let ws_clients = {};
let ws_clients_errors = {};
let globals = {last_process_time:null};
module.exports.globals = globals;

function set_bot(bot_obj) {
    bot = bot_obj;
}
module.exports.set_bot = set_bot;

async function process1s() {
    try {
        await process_trading1s();
    } catch (err) {
        util.error('Error: process_trading1s failed: ' + err.message + ' ' + util.safeStringify(err.stack));
        console.log(err);
    }
}
module.exports.process1s = process1s;


async function health_check() {
    //console.log('in health_check');
    let db_query = {
        name: 'bina.f_health_check',
        text: 'SELECT bina.f_health_check() as av_error',
        values: []
    };

    let res;
    try {
        res = await pool.query(db_query);
        let error = res.rows[0].av_error;

        if (error) {
            await notify_event({telegram_id: conf.admin_telegram_id}, error, 'system');
            //await bot.telegram.sendMessage(conf.admin_telegram_id, util.fixTgMarkup(error), {parse_mode: 'Markdown', disable_web_page_preview: true});
            await util.error('health_check error: ' + error);
            await util.log('restarting...');
            await util.sleep(1000);
            await process.exit(1);
            await util.sleep(1000);
            await process.abort();
        }

        let cred = await get_all_credentials();
        if (cred) {
            for (let i = 0; i < cred.length; i++) {
                if (!ws_clients[cred[i].binance_acc.apikey]) {
                    util.log('health_check connect_ws reconnecting:' + cred[i].subscriber_id);
                    await connect_ws(cred[i].subscriber_id, cred[i].binance_acc);
                    await util.sleep(1000);
                }
            }
        }
    } catch (err) {
        util.error('error in health_check ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.health_check = health_check;


async function process_trading1s() {

    let data = await get_events1s();

    if (data.last_process_time) {
        globals.last_process_time = data.last_process_time;
        //console.log(util.getFormattedTime() + ' process_auto_trading - ' + data.last_process_time);

        if (data.events) {
            for (let i = 0; i < data.events.length; i++) {
                let event = data.events[i];
                try {
                    if (event.trade_event === 'new_event') {
                        if (!trade_state[event.subscriber_id]) {
                            trade_state[event.subscriber_id] = {};
                        }

                        if (event.symbol_rules.validated && event.balance_usdt >= conf.min_deposit) {
                            //util.log(event.subscriber_id + '>> trade_event: ' + event.trade_event + ' ' + util.safeStringify(event));
                            if (!trade_state[event.subscriber_id].trading) {
                                notify_event(event, 'n' + event.acc_num + ': ' + event.trade_event + ' ' + event.side + ' ' + event.symbol + ' ' + event.cur_dev_pc + '%\n'
                                    + '`' + event.chart + '`'
                                    , 'new_event');

                                if (event.trading_mode === 'A' || event.trading_mode === 'O') {
                                    process_auto_trading(event);
                                }
                            } else {
                                util.log(event.subscriber_id + '>> skip trading: trade already exists ' + util.safeStringify(trade_state[event.subscriber_id]));
                            }
                            trade_state[event.subscriber_id].prev_blocked_symbol = null;
                        } else {
                            //bina.trade_log(event.subscriber_id, 'skipping trading event', event);
                            if (!event.symbol_rules.validated) {
                                if (event.symbol !== trade_state[event.subscriber_id].prev_blocked_symbol) {
                                    notify_event(event, 'n' + event.acc_num + ':blocked event ' + event.side + ' ' + event.symbol + ' ' + event.symbol_rules.reasons + ' ' + event.cur_dev_pc + '%\n'
                                        + '`' + event.chart + '`\n'
                                        , 'blocked_symbol');
                                }
                                trade_state[event.subscriber_id].prev_blocked_symbol = event.symbol;
                            } else {
                                trade_state[event.subscriber_id].prev_blocked_symbol = null;
                            }
                        }

                    } else if (event.trade_event === 'stop_loss_move_to_zero'
                        && trade_state[event.subscriber_id].trading
                        && !trade_state[event.subscriber_id].stop_loss.moved_to_zero
                        && !trade_state[event.subscriber_id].stop_loss.moving_to_zero) {
                            trade_state[event.subscriber_id].stop_loss.moving_to_zero = true;
                            util.log(event.subscriber_id + '>> trade_event: ' + event.trade_event + ' event:' + util.safeStringify(event));
                            stop_loss_move_to_zero(trade_state[event.subscriber_id].event);
                    }
                } catch (err) {
                    util.error(event.subscriber_id + '>> error in process_auto_trading ' + err.message + ' ' + util.safeStringify(err.stack));
                }
            }
        }
    }
}


async function get_events1s() {
    //console.log('in get_events1s');
    let db_query = {
        name: 'bina.f_get_events1s',
        text: 'SELECT aj_events, av_last_process_time from bina.f_get_events1s()',
        values: []
    };

    let res;
    try {
        res = await poole.query(db_query);
        return {last_process_time: res.rows[0].av_last_process_time, events: res.rows[0].aj_events};
    } catch (err) {
        util.error('error in get_events1s ' + err.message + ' ' + util.safeStringify(err.stack));
        return {last_process_time: null, events: null};
    }
    //console.log('ad_last_process_time ' + res.rows[0].av_last_process_time);
}


async function start_trading(event) {
    //console.log('in start_trading');
    let db_query = {
        name: 'bina.f_start_trading',
        text: 'SELECT bina.f_start_trading($1, $2, $3) av_error',
        values: [event.subscriber_id, event.symbol, event.side]
    };

    let res;
    try {
        res = await pool.query(db_query);
        return res.rows[0].av_error;
    } catch (err) {
        util.error('error in start_trading ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'error in start_trading';
    }
}

async function save_trade_state(subscriber_id) {
    let db_query = {
        name: 'bina.f_save_trade_state',
        text: 'SELECT bina.f_save_trade_state($1, $2)',
        values: [subscriber_id, trade_state[subscriber_id]]
    };

    let res;
    try {
        res = await pool.query(db_query);
        //return res.rows[0].av_error;
    } catch (err) {
        util.error('error in save_trade_state ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'error in save_trade_state';
    }
}

async function notify_event(event, message, type) {
    try {
        message = message + ' at ' + util.getFormattedTime();
        bina.trade_log(event.subscriber_id, 'notify event', {telegram_id: event.telegram_id, type:type, message: message});
        message = util.fixTgMarkup(message);
        if (!type || !event.notifications || event.notifications[type]) {
            await bot.telegram.sendMessage(event.telegram_id, message, {
                parse_mode: 'Markdown',
                disable_web_page_preview: true
            });
        }
    } catch (err) {
        util.log(event.subscriber_id + '>> internal exception in notify_event ' + event.telegram_id + ' ' + message + ' ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function initial_order(event) {
    try {
        let order, initial_price, order_usdt, balance_change_usdt, new_leverage, new_order_balance_pc, add_dev_pc;

        if (event.side === 'SELL') {
            new_order_balance_pc = event.config.short.new_short_order_balance_pc;
            add_dev_pc = event.config.short.add_dev_short_pc;
            initial_price = event.price * (1 + add_dev_pc / 100);
            new_leverage = event.config.short.leverage_short;
        } else {
            new_order_balance_pc = event.config.long.new_long_order_balance_pc;
            add_dev_pc = event.config.long.add_dev_long_pc;
            initial_price = event.price * (1 - add_dev_pc / 100);
            new_leverage = event.config.long.leverage_long;
        }
        if (new_leverage > event.binance_leverage) {
            new_leverage = event.binance_leverage;
        }

        trade_state[event.subscriber_id].position.side = event.side;
        trade_state[event.subscriber_id].leverage = new_leverage;
        trade_state[event.subscriber_id].symbol = event.symbol;
        initial_price = bina.roundTickSize(initial_price, event.exchange_info.filters[0].tickSize);
        initial_price = bina.trimToDecimalPlaces(initial_price, event.exchange_info.price_precision);
        trade_state[event.subscriber_id].position.entry_price = initial_price; // will be ajusted after filled event
        save_trade_state(event.subscriber_id);

        order_usdt = trade_state[event.subscriber_id].balance_usdt * new_leverage * new_order_balance_pc / 100;
        balance_change_usdt = trade_state[event.subscriber_id].balance_usdt * new_order_balance_pc / 100;
        bina.trade_log(event.subscriber_id,'create initial order', {symbol:event.symbol, side:event.side, initial_price:initial_price, current_price:event.price, position_entry_price:trade_state[event.subscriber_id].position.entry_price, add_dev_pc:add_dev_pc, new_order_balance_pc:new_order_balance_pc, leverage:new_leverage, order_usdt:order_usdt, tick_size:event.exchange_info.filters[0].tickSize, price_precision:event.exchange_info.price_precision, quantity_precision:event.exchange_info.quantity_precision});

        trade_state[event.subscriber_id].initial.order_id = null;
        order = await bina.orderLimit(event.subscriber_id, event.side, event.symbol, order_usdt, null, event.price, initial_price, false, event.binance_acc, event.exchange_info, 'initial');
        bina.trade_log(event.subscriber_id,'create initial order result', order);
        if (order && order.orderId) {
            trade_state[event.subscriber_id].initial.order_id = order.orderId;
            trade_state[event.subscriber_id].initial.balance_change_usdt = balance_change_usdt;
            trade_state[event.subscriber_id].initial.canceled = false;
            trade_state[event.subscriber_id].balance_usdt = trade_state[event.subscriber_id].balance_usdt - balance_change_usdt;
            notify_event(event, 'n' + event.acc_num + ': initial order created #' + order.orderId + ': ' + order.side + ' ' + order.origQty + ' ' + order.price, 'created_order');
        } else {
            bina.trade_log(event.subscriber_id, 'end trading', {symbol: trade_state[event.subscriber_id].symbol, reason: 'failed_order'});
            trade_state[event.subscriber_id].trading = false;
            notify_event(event, 'n' + event.acc_num + ': initial order failed: ' + order.message, 'failed_order');
        }
        save_trade_state(event.subscriber_id);
    } catch(err) {
        bina.trade_log(event.subscriber_id, 'end trading', {symbol: trade_state[event.subscriber_id].symbol, reason: 'exception'});
        trade_state[event.subscriber_id].trading = false;
        util.log('initial order failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function average_order(event) {
    try {
        let order, avg_leverage, avg_order_balance_pc, avg_dev_pc, average_price, order_usdt, balance_change_usdt;

        if (event.side === 'SELL') {
            avg_order_balance_pc = event.config.short.avg_short_order_balance_pc;
            avg_leverage = event.config.short.leverage_short;
            avg_dev_pc = event.config.short.avg_dev_short_pc;
            average_price = trade_state[event.subscriber_id].initial.entry_price * (1 + avg_dev_pc / 100);
        } else {
            avg_order_balance_pc = event.config.long.avg_long_order_balance_pc;
            avg_leverage = event.config.long.leverage_long;
            avg_dev_pc = event.config.long.avg_dev_long_pc;
            average_price = trade_state[event.subscriber_id].initial.entry_price * (1 - avg_dev_pc / 100);
        }

        if (avg_order_balance_pc > 0) {
            average_price = bina.roundTickSize(average_price, event.exchange_info.filters[0].tickSize);
            average_price = bina.trimToDecimalPlaces(average_price, event.exchange_info.price_precision);

            order_usdt = trade_state[event.subscriber_id].balance_usdt * avg_leverage * avg_order_balance_pc / 100;
            balance_change_usdt = trade_state[event.subscriber_id].balance_usdt * avg_order_balance_pc / 100;
            bina.trade_log(event.subscriber_id, 'create average order', {
                symbol: event.symbol,
                side: event.side,
                average_price: average_price,
                current_price: event.price,
                position_entry_price: trade_state[event.subscriber_id].position.entry_price,
                avg_dev_pc: avg_dev_pc,
                avg_order_balance_pc: avg_order_balance_pc,
                leverage: avg_leverage,
                order_usdt: order_usdt,
                tick_size: event.exchange_info.filters[0].tickSize,
                price_precision: event.exchange_info.price_precision
            });

            trade_state[event.subscriber_id].average.order_id = null;
            order = await bina.orderLimit(event.subscriber_id, event.side, event.symbol, order_usdt, null, event.price, average_price, false, event.binance_acc, event.exchange_info, 'average');
            bina.trade_log(event.subscriber_id, 'create average order result', order);
            if (order && order.orderId) {
                trade_state[event.subscriber_id].average.order_id = order.orderId;
                trade_state[event.subscriber_id].average.balance_change_usdt = balance_change_usdt;
                trade_state[event.subscriber_id].average.canceled = false;
                trade_state[event.subscriber_id].balance_usdt = trade_state[event.subscriber_id].balance_usdt - balance_change_usdt;
                notify_event(event, 'n' + event.acc_num + ': average order created #' + order.orderId + ': ' + order.side + ' ' + order.origQty + ' ' + order.price, 'created_order');
            } else {
                notify_event(event, 'n' + event.acc_num + ': average order failed: ' + order.message, 'failed_order');
            }
            save_trade_state(event.subscriber_id);
        }
    } catch(err) {
        util.log('average order failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function third_order(event) {
    try {
        let order, order_usdt, balance_change_usdt, third_price, third_leverage, third_order_balance_pc, third_dev_pc;

        if (event.side === 'SELL') {
            third_order_balance_pc = event.config.short.third_short_order_balance_pc;
            third_leverage = event.config.short.leverage_short;
            third_dev_pc = event.config.short.third_dev_short_pc;
            third_price = trade_state[event.subscriber_id].average.entry_price * (1 + third_dev_pc / 100);
        } else {
            third_order_balance_pc = event.config.long.third_long_order_balance_pc;
            third_leverage = event.config.long.leverage_long;
            third_dev_pc = event.config.long.third_dev_long_pc;
            third_price = trade_state[event.subscriber_id].average.entry_price * (1 - third_dev_pc / 100);
        }

        if (third_order_balance_pc > 0) {
            third_price = bina.roundTickSize(third_price, event.exchange_info.filters[0].tickSize);
            third_price = bina.trimToDecimalPlaces(third_price, event.exchange_info.price_precision);

            order_usdt = trade_state[event.subscriber_id].balance_usdt * third_leverage * third_order_balance_pc / 100;
            balance_change_usdt = trade_state[event.subscriber_id].balance_usdt * third_order_balance_pc / 100;
            bina.trade_log(event.subscriber_id, 'create third order', {
                symbol: event.symbol,
                side: event.side,
                third_price: third_price,
                current_price: event.price,
                position_entry_price: trade_state[event.subscriber_id].position.entry_price,
                third_dev_pc: third_dev_pc,
                third_order_balance_pc: third_order_balance_pc,
                leverage: third_leverage,
                order_usdt: order_usdt,
                tick_size: event.exchange_info.filters[0].tickSize,
                price_precision: event.exchange_info.price_precision
            });

            trade_state[event.subscriber_id].third.order_id = null;
            order = await bina.orderLimit(event.subscriber_id, event.side, event.symbol, order_usdt, null, event.price, third_price, false, event.binance_acc, event.exchange_info, 'third');
            bina.trade_log(event.subscriber_id, 'create third order result', order);
            if (order && order.orderId) {
                trade_state[event.subscriber_id].third.order_id = order.orderId;
                trade_state[event.subscriber_id].third.balance_change_usdt = balance_change_usdt;
                trade_state[event.subscriber_id].third.canceled = false;
                trade_state[event.subscriber_id].balance_usdt = trade_state[event.subscriber_id].balance_usdt - balance_change_usdt;
                notify_event(event, 'n' + event.acc_num + ': third order created #' + order.orderId + ': ' + order.side + ' ' + order.origQty + ' ' + order.price, 'created_order');
            } else {
                notify_event(event, 'n' + event.acc_num + ': third order failed: ' + order.message, 'created_order');
            }
            save_trade_state(event.subscriber_id);
        }
    } catch (err) {
        util.log('third order failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}


function get_trade_event(subscriber_id) {
    return trade_state[subscriber_id].event;
}
module.exports.get_trade_event = get_trade_event;


async function process_auto_trading(event) {
    trade_state[event.subscriber_id].trading = true;
    bina.trade_log(event.subscriber_id,'start trading', {symbol: event.symbol, side:event.side, dev_pc:event.cur_dev_pc, ema_index:event.ema_index});
    let error = await start_trading(event);
    if (error) {
        bina.trade_log(event.subscriber_id,'end trading', {symbol: event.symbol, reason: error});
        trade_state[event.subscriber_id].trading = false;
        save_trade_state(event.subscriber_id);
        return;
    }

    let now = new Date();
    trade_state[event.subscriber_id] = {
        trading: true, leverage: null, balance_usdt:event.balance_usdt,
        start_dt: Math.floor(now.getTime() / 1000), cancel_dt: null,
        position: {side: null, entry_price: null, amount: null},
        initial: {order_id:null, entry_price:null, side: null, opened:false, canceled:false, amount:0, filled_amount:0},
        average: {order_id:null, entry_price:null, side: null, opened:false, canceled:false, amount:0, filled_amount:0},
        third: {order_id:null, entry_price:null, side: null, opened:false, canceled:false, amount:0, filled_amount:0},
        take_profit: {order_id:null, entry_price:null, side: null, opened:false, canceled:false, amount:0, filled_amount:0},
        stop_loss: {order_id:null, entry_price:null, side: null, opened:false, canceled:false, amount:0, filled_amount:0},
        undefined_type: {order_id:null, entry_price:null, side: null, opened:false, canceled:false, amount:0, filled_amount:0},
        event: event
    };

    if (!ws_clients[event.binance_acc.apikey]) {
        await connect_ws(event.subscriber_id, event.binance_acc);
    }

    if (event.binance_leverage !== event.current_binance_leverage) {
        bina.trade_log(event.subscriber_id,'change binance leverage', {symbol:event.symbol, current_binance_leverage: event.current_binance_leverage, new_leverage: event.binance_leverage});
        await bina.setLeverage(event.symbol, event.binance_leverage, event.binance_acc);
    }

    await initial_order(event);

    let cancel_new_order_sec;
    if (event.side === 'SELL') {
        cancel_new_order_sec = event.config.short.cancel_new_short_order_sec;
    } else {
        cancel_new_order_sec = event.config.long.cancel_new_long_order_sec;
    }
    trade_state[event.subscriber_id].cancel_dt = trade_state[event.subscriber_id].start_dt + cancel_new_order_sec;
    save_trade_state(event.subscriber_id);

    await util.sleep(cancel_new_order_sec * 1000);

    //positions have not yet opened (after cancel_new_order_sec) so lets cancel them
    if (!trade_state[event.subscriber_id].initial.opened) {
        bina.trade_log(event.subscriber_id,'end trading', {symbol: event.symbol, error: 'initial_order_timeout'});
        trade_state[event.subscriber_id].trading = false;
        await cancelActiveOrders(event, 'initial_order_timeout');
        setTimeout(actualize_state,1000, event);
    }
}


// cancel order if exists
async function cancel_order(event, order_type) {
    try {
        util.log(event.subscriber_id + '>> in cancel_order ' + order_type + ' ' + util.safeStringify(trade_state[event.subscriber_id][order_type]));

        if (trade_state[event.subscriber_id][order_type].order_id
            && !trade_state[event.subscriber_id][order_type].opened
            && !trade_state[event.subscriber_id][order_type].canceled) {

            bina.trade_log(event.subscriber_id, 'canceling order ' + order_type, {
                symbol: event.symbol,
                order_id: trade_state[event.subscriber_id][order_type].order_id
            });
            let is_success = await bina.cancelOrder(event.symbol, trade_state[event.subscriber_id][order_type].order_id, event.binance_acc);
            if (is_success) {
                trade_state[event.subscriber_id][order_type].canceled = true;
                save_trade_state(event.subscriber_id);
                notify_event(event, 'n' + event.acc_num + ': cancel ' + order_type + ' order #' + trade_state[event.subscriber_id][order_type].order_id + ': ' + event.side, 'canceled_order');
            }
            return is_success;
        } else {
            return true;
        }
    } catch(err) {
        util.log('cancel_order failed: ' + err.message + ' ' + util.safeStringify(err.stack));
        return false;
    }
}

// cancel all orders
async function cancelActiveOrders(event, reason) {
    bina.trade_log(event.subscriber_id,'cancel_all_active_orders', {symbol: event.symbol, reason: reason});
    try {
        let orders = await bina.getActiveOrd(event.binance_acc);
        if (orders && orders.length > 0) {
            for (const order of orders) {
                util.log(event.subscriber_id + '>> canceling order #' + order.orderId + ' ' + order.symbol);
                await bina.cancelOrder(order.symbol, order.orderId, event.binance_acc);
            }
        }
    } catch (er) {
        util.log('cancelActiveOrders failed: ' + err.message + ' ' + util.safeStringify(err.stack));
        return false;
    }
}
module.exports.cancelActiveOrders = cancelActiveOrders;


// stop command
async function stop_trading(subscriber_id, binance_acc, reason) {
    bina.trade_log(subscriber_id,'end trading', {symbol: trade_state[subscriber_id].symbol, reason: reason});
    trade_state[subscriber_id].trading = false;

    await bina.closeActivePosition(subscriber_id, binance_acc,'stop');
    await cancelActiveOrders({subscriber_id: subscriber_id, binance_acc}, 'stop command');
    setTimeout(actualize_state,1000, trade_state[subscriber_id].event);
}
module.exports.stop_trading = stop_trading;


async function get_all_credentials() {
    let db_query = {
        name: 'bina.f_get_all_credentials',
        text: 'SELECT bina.f_get_all_credentials() atj_credentials',
        values: []
    };

    let res;
    try {
        res = await pool.query(db_query);
        //console.log(res.rows[0].atj_credentials);

        return res.rows[0].atj_credentials;
    } catch (err) {
        util.error('error in get_all_credentials ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

let unique_msg = {};
function is_unique_order_message(subscriber_id, str_order) {
    let key = MD5(str_order).toString();

    if (!unique_msg[key]) {
        unique_msg[key] = 0;
    }
    unique_msg[key]++;

    if (unique_msg[key] > 1) {
        util.log(subscriber_id + '>> skip-duplicated-message ' + unique_msg[key] + ': ' + str_order);
        return false;
    } else {
        util.log(subscriber_id + '>> new-message: ' + str_order);
        return true;
    }
}

async function connect_ws(subscriber_id, binance_acc) {
    let is_cancel_success;
    let apikey = binance_acc.apikey;
    let secretkey = binance_acc.secretkey;

    try {
        if (ws_clients_errors[subscriber_id] >= 3) {
            util.log(subscriber_id + '>> auto restarting after fails:' + ws_clients_errors[subscriber_id]);
            await util.sleep(1000);
            await process.exit(1);
            await util.sleep(1000);
            await process.abort();
            return;
        }


        util.log(subscriber_id + '>> ws terminating... ' + apikey.substring(0, 10));
        if (ws_clients[apikey]) {
            try {
                await ws_clients[apikey].closeWs();
            } catch (err) {
                util.log(subscriber_id + '>> ws closeWs FAILED ' + apikey.substring(0, 10) + ' ' + err.message + ' ' + util.safeStringify(err.stack));
            }
        }
        ws_clients[apikey] = undefined;

        util.log(subscriber_id + '>> ws connecting to Binance... ' + apikey.substring(0, 10));
        ws_clients[apikey] = await new WebsocketClient({
            api_key: apikey,
            api_secret: secretkey,
            beautify: true
        });
        //util.log(subscriber_id + '>> ws connected...');

        ws_clients[apikey].on('open', (data) => {
            ws_clients[apikey].ws_key = data.wsKey;
            ws_clients[apikey].subscriber_id = subscriber_id;
            ws_clients[apikey].binance_acc = binance_acc;
            util.log(subscriber_id + '>> wsClient opened wsKey:' + data.wsKey);
        });

        ws_clients[apikey].on('formattedMessage', async (data) => {
            try {
                if (data && data.order && data.eventType === 'ORDER_TRADE_UPDATE') {
                    let str_order = util.safeStringify(data.order);
                    if (is_unique_order_message(subscriber_id, str_order)) {

                        let order_type = util.getOrderType(data.order.clientOrderId);
                        let order_sub_type = util.getOrderSubType(data.order.clientOrderId);
                        util.log(subscriber_id + '>> trade-state: ' + util.safeStringify(trade_state[subscriber_id]));

                        if (trade_state[subscriber_id] && trade_state[subscriber_id].trading) {
                            if (data.order.orderStatus === 'NEW') {
                                trade_state[subscriber_id][order_type].order_id = data.order.orderId;
                                trade_state[subscriber_id][order_type].side = data.order.side;
                                trade_state[subscriber_id][order_type].clientOrderId = data.order.clientOrderId;
                                trade_state[subscriber_id][order_type].entry_price = data.order.originalPrice;
                                save_trade_state(subscriber_id);

                                if (order_type === 'initial'
                                    && (trade_state[subscriber_id].event.config.average_start_mode === 'S' || trade_state[subscriber_id].event.config.average_start_mode === 'C')) {
                                    await util.sleep(100);
                                    await average_order(trade_state[subscriber_id].event);
                                } else if (order_type === 'average'
                                    && (trade_state[subscriber_id].event.config.average_start_mode === 'S' || trade_state[subscriber_id].event.config.average_start_mode === 'C')) {
                                    await util.sleep(100);
                                    await third_order(trade_state[subscriber_id].event);
                                }

                            }
                            if (data.order.orderStatus === 'PARTIALLY_FILLED') {
                                if (order_type === 'take_profit' || order_type === 'stop_loss') {
                                    trade_state[subscriber_id][order_type].filled_amount = trade_state[subscriber_id][order_type].filled_amount + data.order.realisedProfit;
                                }
                            }

                            if (data.order.orderStatus === 'FILLED') {
                                let position_amount, position_entry_price;
                                /*let pos = await bina.getPosition(trade_state[subscriber_id].event.symbol, trade_state[subscriber_id].event.binance_acc);
                                if (pos && pos[0] && pos[0].entryPrice > 0) {
                                    position_amount = Math.abs(pos[0].positionAmt);
                                    position_entry_price = pos[0].entryPrice;
                                    trade_state[subscriber_id].position.amount = position_amount;
                                    trade_state[subscriber_id].position.entry_price = position_entry_price;
                                    trade_state[subscriber_id].position.filled_amount = bina.trimToDecimalPlaces(position_amount * position_entry_price, 3);
                                }*/

                                if (order_type === 'initial' || order_type === 'average' || order_type === 'third') {
                                    let pos = await bina.getPosition(trade_state[subscriber_id].event.symbol, trade_state[subscriber_id].event.binance_acc);
                                    bina.trade_log(subscriber_id, 'position filled from ' + order_type + ' order', pos[0]);
                                    if (pos && pos[0] && pos[0].entryPrice > 0) {
                                        position_amount = Math.abs(pos[0].positionAmt);
                                        position_entry_price = pos[0].entryPrice;
                                        trade_state[subscriber_id].position.amount = position_amount;
                                        trade_state[subscriber_id].position.entry_price = position_entry_price;
                                        trade_state[subscriber_id].position.filled_amount = bina.trimToDecimalPlaces(position_amount * position_entry_price, 3);

                                        if (!trade_state[subscriber_id][order_type].opened) {
                                            trade_state[subscriber_id][order_type].opened = true;
                                            trade_state[subscriber_id][order_type].amount = Math.abs(data.order.originalQuantity);
                                            trade_state[subscriber_id][order_type].entry_price = data.order.averagePrice;
                                            trade_state[subscriber_id][order_type].filled_amount = bina.trimToDecimalPlaces(Math.abs(data.order.originalQuantity) * data.order.averagePrice, 3);
                                            trade_state[subscriber_id].stop_loss.moved_to_zero = false;
                                            save_trade_state(subscriber_id);

                                            util.log(subscriber_id + '>> average_start_mode ' + trade_state[subscriber_id].event.config.average_start_mode + ' opened2:' + trade_state[subscriber_id].average.opened + ' opened3:' + trade_state[subscriber_id].third.opened);
                                            if (order_type === 'initial' && !trade_state[subscriber_id].average.opened) {
                                                if (trade_state[subscriber_id].event.config.average_start_mode === 'C') {
                                                    /*is_cancel_success = await cancel_order(trade_state[subscriber_id].event, 'third');
                                                    if (is_cancel_success) {
                                                        trade_state[subscriber_id].balance_usdt = trade_state[subscriber_id].balance_usdt + trade_state[subscriber_id].third.balance_change_usdt;
                                                        util.log(subscriber_id + '>> third: is_cancel_success ' + is_cancel_success);
                                                    }*/
                                                    is_cancel_success = await cancel_order(trade_state[subscriber_id].event, 'average');
                                                    if (is_cancel_success) {
                                                        if (trade_state[subscriber_id].average.balance_change_usdt !== 0) {
                                                            trade_state[subscriber_id].balance_usdt = trade_state[subscriber_id].balance_usdt + trade_state[subscriber_id].average.balance_change_usdt;
                                                        }
                                                        util.log(subscriber_id + '>> average: is_cancel_success ' + is_cancel_success);
                                                    }
                                                }
                                                if (trade_state[subscriber_id].event.config.average_start_mode === 'P'
                                                    || (trade_state[subscriber_id].event.config.average_start_mode === 'C' && is_cancel_success)) {
                                                    await average_order(trade_state[subscriber_id].event);
                                                    //await third_order(trade_state[subscriber_id].event);
                                                }
                                            } else if (order_type === 'average' && !trade_state[subscriber_id].third.opened) {
                                                if (trade_state[subscriber_id].event.config.average_start_mode === 'C') {
                                                    is_cancel_success = await cancel_order(trade_state[subscriber_id].event, 'third');
                                                    if (is_cancel_success) {
                                                        if (trade_state[subscriber_id].third.balance_change_usdt !== 0) {
                                                            trade_state[subscriber_id].balance_usdt = trade_state[subscriber_id].balance_usdt + trade_state[subscriber_id].third.balance_change_usdt;
                                                        }
                                                        util.log(subscriber_id + '>> third: is_cancel_success ' + is_cancel_success);
                                                    }
                                                }
                                                if (trade_state[subscriber_id].event.config.average_start_mode === 'P'
                                                    || (trade_state[subscriber_id].event.config.average_start_mode === 'C' && is_cancel_success)) {
                                                    await third_order(trade_state[subscriber_id].event);
                                                }
                                            }

                                            bina.trade_log(subscriber_id, 'position opened by ' + order_type, {
                                                order_id: trade_state[subscriber_id][order_type].order_id,
                                                position_entry_price: position_entry_price,
                                                position_amount: position_amount
                                            });
                                            notify_event(trade_state[subscriber_id].event, 'n' + trade_state[subscriber_id].event.acc_num + ': position opened by ' + order_type + ' position_entry_price: ' + position_entry_price + ' '
                                                + trade_state[subscriber_id].event.side + ' ' + trade_state[subscriber_id].event.symbol + ' ' + bina.trimToDecimalPlaces(trade_state[subscriber_id][order_type].filled_amount, 3) + '$', 'opened_position');
                                            await create_tp_sl_orders(trade_state[subscriber_id].event, order_type);
                                        }
                                    }
                                } else if ((order_type === 'take_profit' && (order_sub_type !== 'partial' || order_sub_type === ''))
                                        //&& (order_sub_type === 'full' || order_sub_type === 'second' || order_sub_type === 'trailing' || order_sub_type === 'move_to_zero')
                                        || order_type === 'stop_loss') {
                                    bina.trade_log(subscriber_id, 'end trading', {
                                        symbol: trade_state[subscriber_id].symbol,
                                        reason: 'filled ' + order_type + ' sub_type ' + order_sub_type
                                    });
                                    trade_state[subscriber_id].trading = false;
                                    if (!trade_state[subscriber_id][order_type].opened) {
                                        trade_state[subscriber_id][order_type].opened = true; // it prevents cancelling and moving to zero
                                        trade_state[subscriber_id][order_type].filled_amount = trade_state[subscriber_id][order_type].filled_amount + data.order.realisedProfit;
                                    }
                                    save_trade_state(subscriber_id);

                                    let trade_event = await bina.trade_log(subscriber_id, 'filled_order_' + order_type, {
                                        symbol: data.order.symbol,
                                        order_id: trade_state[subscriber_id][order_type].order_id,
                                        filled_amount: trade_state[subscriber_id][order_type].filled_amount
                                    });

                                    //after fill take_profit/stop_loss order lets cancel all orders
                                    await cancelActiveOrders(trade_state[subscriber_id].event, order_type);

                                    await util.sleep(1000); // wait for balance update
                                    let new_balance = await bina.getBalances(trade_state[subscriber_id].event.binance_acc);
                                    notify_event(trade_state[subscriber_id].event,
                                        'n' + trade_state[subscriber_id].event.acc_num
                                        + ': realised ' + order_type + '.' + order_sub_type + ' ' + bina.trimToDecimalPlaces(trade_state[subscriber_id][order_type].filled_amount, 3)
                                        + '$ on ' + trade_state[subscriber_id].event.symbol
                                        + '\nbalance: ' + new_balance, 'realised_profit');

                                    if (trade_event === 'bot_paused') {
                                        notify_event(trade_state[subscriber_id].event, 'n' + trade_state[subscriber_id].event.acc_num + ': bot paused', '');
                                    }

                                } else if (order_type === 'take_profit' && order_sub_type === 'partial') {
                                    trade_state[subscriber_id].position.amount = bina.trimToDecimalPlaces(trade_state[subscriber_id].position.amount - Math.abs(data.order.originalQuantity), trade_state[subscriber_id].event.exchange_info.quantity_precision);
                                    if (!trade_state[subscriber_id].take_profit.opened) {
                                        trade_state[subscriber_id].take_profit.opened = true; // it prevents cancelling and moving to zero
                                        trade_state[subscriber_id].take_profit.filled_amount = trade_state[subscriber_id][order_type].filled_amount + data.order.realisedProfit;
                                    }
                                    save_trade_state(subscriber_id);

                                    bina.trade_log(subscriber_id, 'filled_order_' + order_type + '_' + order_sub_type, {
                                        symbol: data.order.symbol,
                                        order_id: trade_state[subscriber_id].take_profit.order_id,
                                        filled_amount: trade_state[subscriber_id].take_profit.filled_amount
                                    });

                                    notify_event(trade_state[subscriber_id].event,
                                        'n' + trade_state[subscriber_id].event.acc_num
                                        + ': realised ' + order_type + '.' + order_sub_type + ' ' + bina.trimToDecimalPlaces(trade_state[subscriber_id].take_profit.filled_amount, 3)
                                        + '$ on ' + trade_state[subscriber_id].event.symbol, 'realised_profit');

                                    let take_profit_pc, second_take_profit_add_pc, second_take_profit_price, trailing_stop_callback_pc;
                                    if (trade_state[subscriber_id].event.side === 'SELL') {
                                        take_profit_pc = trade_state[subscriber_id].event.config.short.take_profit_short_pc;
                                        second_take_profit_add_pc = trade_state[subscriber_id].event.config.short.second_take_profit_short_add_pc;
                                        second_take_profit_price = trade_state[subscriber_id].position.entry_price * (1 - (take_profit_pc+second_take_profit_add_pc) / 100);
                                        trailing_stop_callback_pc = trade_state[subscriber_id].event.config.short.trailing_stop_short_callback_pc;
                                    } else {
                                        take_profit_pc = trade_state[subscriber_id].event.config.long.take_profit_long_pc;
                                        second_take_profit_add_pc = trade_state[subscriber_id].event.config.long.second_take_profit_long_add_pc;
                                        second_take_profit_price = trade_state[subscriber_id].position.entry_price * (1 + (take_profit_pc+second_take_profit_add_pc) / 100);
                                        trailing_stop_callback_pc = trade_state[subscriber_id].event.config.long.trailing_stop_long_callback_pc;
                                    }

                                    // for proper operating second take_profit order: when FILLED take_profit.trailing
                                    trade_state[subscriber_id].take_profit.filled_amount = 0;
                                    trade_state[subscriber_id].take_profit.opened = false;

                                    if (trailing_stop_callback_pc > 0) {
                                        //trailing after take_profit.partial
                                        await trailing(trade_state[subscriber_id].event, order_type + '.' + order_sub_type);
                                    } else {
                                        let second_take_profit_order_quantity = trade_state[subscriber_id].position.amount;
                                        second_take_profit_price = bina.roundTickSize(second_take_profit_price, trade_state[subscriber_id].event.exchange_info.filters[0].tickSize);
                                        second_take_profit_price = bina.trimToDecimalPlaces(second_take_profit_price, trade_state[subscriber_id].event.exchange_info.price_precision);

                                        if (second_take_profit_add_pc > 0 && second_take_profit_order_quantity > 0) {
                                            bina.trade_log(subscriber_id, 'second_take_profit by ' + order_type + ' order', {
                                                symbol: trade_state[subscriber_id].event.symbol,
                                                side: trade_state[subscriber_id].event.close_side,
                                                take_profit_pc: take_profit_pc + second_take_profit_add_pc,
                                                take_profit_price: second_take_profit_price,
                                                order_quantity: second_take_profit_order_quantity
                                            });
                                            let order = await bina.orderLimit(subscriber_id, trade_state[subscriber_id].event.close_side, trade_state[subscriber_id].event.symbol, null, second_take_profit_order_quantity, null, second_take_profit_price, true, trade_state[subscriber_id].event.binance_acc, trade_state[subscriber_id].event.exchange_info, 'take_profit.second');
                                            bina.trade_log(subscriber_id, 'second_take_profit by ' + order_type + ' order result', order);
                                            if (order && order.orderId) {
                                                trade_state[subscriber_id].take_profit.order_id = order.orderId;
                                                trade_state[subscriber_id].take_profit.canceled = false;
                                                save_trade_state(subscriber_id);
                                                notify_event(trade_state[subscriber_id].event, 'n' + trade_state[subscriber_id].event.acc_num + ': second_take_profit by ' + order_type + ' created #' + order.orderId + ': ' + order.side + ' ' + order.origQty + ' ' + order.price, 'created_order');
                                            } else {
                                                notify_event(trade_state[subscriber_id].event, 'n' + trade_state[subscriber_id].event.acc_num + ': second_take_profit by ' + order_type + ' failed: ' + order.message, 'failed_order');
                                            }
                                        }
                                    }

                                    setTimeout(actualize_state,1000, trade_state[subscriber_id].event);
                                }
                            }
                            /*if (data.order.orderStatus === 'CANCELED') {
                                trade_state[subscriber_id][order_type].canceled = true;
                                save_trade_state(subscriber_id);
                            }*/

                            data.order.order_type = order_type;
                            bina.trade_log(subscriber_id, 'order trade update', data.order);

                        } else {
                            data.order.order_type = order_type;
                            bina.trade_log(subscriber_id, 'order outside bot-trading update', data.order);

                            if (data.order.orderStatus === 'FILLED') {
                                let position_amount, position_entry_price;
                                let pos = await bina.getPosition(trade_state[subscriber_id].event.symbol, trade_state[subscriber_id].event.binance_acc);
                                if (pos && pos[0] && pos[0].entryPrice > 0) {
                                    position_amount = Math.abs(pos[0].positionAmt);
                                    position_entry_price = pos[0].entryPrice;
                                    trade_state[subscriber_id].position.amount = position_amount;
                                    trade_state[subscriber_id].position.entry_price = position_entry_price;
                                    trade_state[subscriber_id].position.filled_amount = position_amount * position_entry_price;
                                }

                                if ((order_type === 'take_profit' && (order_sub_type !== 'partial' || order_sub_type === ''))
                                    || order_type === 'stop_loss') {
                                    let opened_before = trade_state[subscriber_id][order_type].opened;
                                    if (!trade_state[subscriber_id][order_type].opened) {
                                        trade_state[subscriber_id][order_type].opened = true; // it prevents cancelling and moving to zero
                                        trade_state[subscriber_id][order_type].filled_amount = trade_state[subscriber_id][order_type].filled_amount + data.order.realisedProfit;
                                    }
                                    save_trade_state(subscriber_id);

                                    //after fill take_profit/stop_loss order lets cancel all orders
                                    await cancelActiveOrders(trade_state[subscriber_id].event, order_type);

                                    if (!opened_before && trade_state[subscriber_id][order_type].opened) {
                                        await util.sleep(1000); // wait for balance update
                                        let new_balance = await bina.getBalances(trade_state[subscriber_id].event.binance_acc);
                                        notify_event(trade_state[subscriber_id].event,
                                            'n' + trade_state[subscriber_id].event.acc_num
                                            + ': realised ' + order_type + ' ' + bina.trimToDecimalPlaces(trade_state[subscriber_id][order_type].filled_amount, 3)
                                            + '$ on ' + trade_state[subscriber_id].event.symbol
                                            + '\nbalance: ' + new_balance, 'realised_profit');
                                    }

                                    setTimeout(actualize_state, 1000, trade_state[subscriber_id].event);
                                }
                            }
                        }

                    }
                }
            } catch (err) {
                util.log(subscriber_id + '>> wsClient order trade update error' + err.message + ' ' + util.safeStringify(err.stack));
            }
        });

        ws_clients[apikey].on('reconnecting', (data) => {
            util.log(subscriber_id + '>> wsClient reconnecting ' + apikey.substring(0, 10) + ' ' + util.safeStringify(data.error));
        });
        ws_clients[apikey].on('reconnected', (data) => {
            util.log(subscriber_id + '>> wsClient reconnected ' + apikey.substring(0, 10) + ' ' + util.safeStringify(data.error));
        });
        ws_clients[apikey].on('error', (data) => {
            util.log(subscriber_id + '>> wsClient error ' + apikey.substring(0, 10) + ' ' + util.safeStringify(data.error));
            ws_clients[apikey] = undefined;
            ws_clients_errors[subscriber_id] = (ws_clients_errors[subscriber_id])?ws_clients_errors[subscriber_id]+1:1;
            util.log(subscriber_id + '>> ws_clients_errors:' + ws_clients_errors[subscriber_id]);
            setTimeout(connect_ws, 1000, subscriber_id, binance_acc);
        });
        ws_clients[apikey].on('close', (data) => {
            util.log(subscriber_id + '>> wsClient close ' + apikey.substring(0, 10) + ' ' + util.safeStringify(data.error));
            ws_clients[apikey] = undefined;
            ws_clients_errors[subscriber_id] = (ws_clients_errors[subscriber_id])?ws_clients_errors[subscriber_id]+1:1;
            util.log(subscriber_id + '>> ws_clients_errors:' + ws_clients_errors[subscriber_id]);
            setTimeout(connect_ws, 1000, subscriber_id, binance_acc);
        });

        await ws_clients[apikey].subscribeUsdFuturesUserDataStream();
        util.log(subscriber_id + '>> ws subscribed...');

        /*let endpoints = binance.websockets.subscriptions();
        for ( let endpoint in endpoints ) {
            util.log(endpoint);
            //let ws = endpoints[endpoint];
            //ws.terminate();
        }*/

    } catch (err) {
        util.log(subscriber_id + '>> ws connecting FAILED ' + apikey.substring(0, 10) + ' ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function actualize_state(event) {
    util.log('subscriber: ' + event.subscriber_id);
    if (!trade_state[event.subscriber_id]) {
        trade_state[event.subscriber_id] = {};
    }
    trade_state[event.subscriber_id].trading = await bina.actualize_state(event);
    await save_trade_state(event.subscriber_id);
}

async function warming_ws_all() {
    util.log('warming_ws_all');

    let cred = await get_all_credentials();
    if (cred) {
        for (let i = 0; i < cred.length; i++) {
            await connect_ws(cred[i].subscriber_id, cred[i].binance_acc);
        }
    }
}
module.exports.warming_ws_all = warming_ws_all;


// after initial position opening we can make orders for take_profit and stop_loss
async function create_tp_sl_orders(event, order_type) {
    let position_amount, order_sub_type, take_profit_pc, take_profit_price, stop_loss_pc, stop_loss_price, avg_order_balance_pc, third_order_balance_pc;
    let first_take_profit_amount_pc, second_take_profit_add_pc, first_take_profit_order_quantity;

    position_amount = trade_state[event.subscriber_id].position.amount;

    //cur_dev_pc / current_ema
    if (event.side === 'SELL') {
        take_profit_pc = event.config.short.take_profit_short_pc;
        first_take_profit_amount_pc = event.config.short.first_take_profit_short_amount_pc;
        second_take_profit_add_pc = event.config.short.second_take_profit_short_add_pc;
        take_profit_price = trade_state[event.subscriber_id].position.entry_price * (1 - take_profit_pc / 100);
        stop_loss_pc = event.config.short.stop_loss_short_pc;
        stop_loss_price = trade_state[event.subscriber_id][order_type].entry_price * (1 + stop_loss_pc / 100);
        avg_order_balance_pc = event.config.short.avg_short_order_balance_pc;
        third_order_balance_pc = event.config.short.third_short_order_balance_pc;
    } else {
        take_profit_pc = event.config.long.take_profit_long_pc;
        first_take_profit_amount_pc = event.config.long.first_take_profit_long_amount_pc;
        second_take_profit_add_pc = event.config.long.second_take_profit_long_add_pc;
        take_profit_price = trade_state[event.subscriber_id].position.entry_price * (1 + take_profit_pc / 100);
        stop_loss_pc = event.config.long.stop_loss_long_pc;
        stop_loss_price = trade_state[event.subscriber_id][order_type].entry_price * (1 - stop_loss_pc / 100);
        avg_order_balance_pc = event.config.long.avg_long_order_balance_pc;
        third_order_balance_pc = event.config.long.third_long_order_balance_pc;
    }

    take_profit_price = bina.roundTickSize(take_profit_price, event.exchange_info.filters[0].tickSize);
    take_profit_price = bina.trimToDecimalPlaces(take_profit_price, event.exchange_info.price_precision);
    stop_loss_price = bina.roundTickSize(stop_loss_price, event.exchange_info.filters[0].tickSize);
    stop_loss_price = bina.trimToDecimalPlaces(stop_loss_price, event.exchange_info.price_precision);

    //stop_loss
    if (order_type !== 'initial') {
        //average or third, so lets cancel prev orders
        await cancel_order(event, 'stop_loss');
    }

    if (stop_loss_pc > 0 && (
        (order_type === 'initial' && avg_order_balance_pc === 0)
        || (order_type === 'average' && third_order_balance_pc === 0)
        || order_type === 'third')) {
        bina.trade_log(event.subscriber_id, 'stop_loss by ' + order_type + ' order', {symbol: event.symbol, order_id: trade_state[event.subscriber_id][order_type].order_id, side: event.close_side, stop_loss_pc:stop_loss_pc, stop_loss_price: stop_loss_price, current_price: event.price, order_quantity: position_amount});
        let order = await bina.orderStopMarket(event.subscriber_id, event.close_side, event.symbol, position_amount, stop_loss_price, event.binance_acc, 'stop_loss');
        bina.trade_log(event.subscriber_id, 'stop_loss by ' + order_type + ' order result', order);
        if (order && order.orderId) {
            trade_state[event.subscriber_id].stop_loss.order_id = order.orderId;
            trade_state[event.subscriber_id].stop_loss.canceled = false;
            save_trade_state(event.subscriber_id);
            notify_event(event, 'n' + event.acc_num + ': stop_loss by ' + order_type + ' created #' + order.orderId + ': ' + order.side + ' ' + order.origQty + ' ' + stop_loss_price, 'created_order');
        } else {
            // -2021 Order would immediately trigger
            // -4131 The counterparty's best price does not meet the PERCENT_PRICE filter limit
            if (order.code === -2021) {
                // lets close by market
                notify_event(event, 'n' + event.acc_num + ': stop_loss failed: too late; close by market', 'failed_order');
                bina.trade_log(event.subscriber_id, 'end trading', {symbol: trade_state[event.subscriber_id].symbol, reason: 'stop_loss.immediate'});
                trade_state[event.subscriber_id].trading = false;
                order = await bina.closeActivePosition(event.subscriber_id, event.binance_acc, 'stop_loss.immediate');
                let interval_ms = 200;
                while (order.code === -4131) {
                    notify_event(event, 'n' + event.acc_num + ': stop_loss.immediate failed: PERCENT_PRICE filter limit; close by market', 'failed_order');
                    await util.sleep(interval_ms);
                    interval_ms += 200;
                    order = await bina.closeActivePosition(event.subscriber_id, event.binance_acc, 'stop_loss.immediate');
                }
                await cancelActiveOrders(event, 'stop_loss_would_immediately_trigger');
                setTimeout(actualize_state,1000, event);
            } else {
                notify_event(event, 'n' + event.acc_num + ': stop_loss by ' + order_type + ' failed: ' + order.message, 'failed_order');
            }
        }
    }

    if (order_type !== 'initial') {
        //average or third, so lets cancel prev orders
        await cancel_order(trade_state[event.subscriber_id].event, 'take_profit');
    }

    //take_profit
    if (take_profit_pc > 0 && first_take_profit_amount_pc > 0) {
        first_take_profit_order_quantity = position_amount * first_take_profit_amount_pc / 100;
        order_sub_type = 'full';
        if (first_take_profit_amount_pc < 100) {
            order_sub_type = 'partial';
        }

        bina.trade_log(event.subscriber_id, 'take_profit by ' + order_type + ' order', {
            symbol: event.symbol,
            order_id: trade_state[event.subscriber_id][order_type].order_id,
            side: event.close_side,
            take_profit_pc: take_profit_pc,
            take_profit_price: take_profit_price,
            current_price: event.price,
            order_quantity: first_take_profit_order_quantity
        });
        let order = await bina.orderLimit(event.subscriber_id, event.close_side, event.symbol, null, first_take_profit_order_quantity, event.price, take_profit_price, true, event.binance_acc, event.exchange_info, 'take_profit.' + order_sub_type);
        bina.trade_log(event.subscriber_id, 'take_profit by ' + order_type + ' order result', order);
        if (order && order.orderId) {
            trade_state[event.subscriber_id].take_profit.order_id = order.orderId;
            trade_state[event.subscriber_id].take_profit.canceled = false;
            save_trade_state(event.subscriber_id);
            notify_event(event, 'n' + event.acc_num + ': take_profit by ' + order_type + ' created #' + order.orderId + ': ' + order.side + ' ' + order.origQty + ' ' + order.price, 'created_order');
        } else {
            notify_event(event, 'n' + event.acc_num + ': take_profit by ' + order_type + ' failed: ' + order.message, 'failed_order');
        }
    }

}

// warming client connections
async function warming_all() {
    //util.log('warming_all');
    let cred = await get_all_credentials();

    if (cred && cred.length) {
        for (let i = 0; i<cred.length; i++) {
            try {
                //let price = await bina.getPrice('BTCUSDT', cred[i].binance_acc);
                await actualize_state(cred[i]);

                /*if (price === 'failed') {
                    await bina.getClient(cred[i].binance_acc, true);
                    price = await bina.getPrice('BTCUSDT', cred[i].binance_acc);
                }*/
                util.log(cred[i].subscriber_id + '>> warming... ' + cred[i].binance_acc.apikey.substring(0,10));
            } catch(err) {
                util.log(cred[i].subscriber_id + '>> warming_all error ' + cred[i].binance_acc.apikey + ' ' + err.message + ' ' + util.safeStringify(err.stack));
            }
        }
    }
}
module.exports.warming_all = warming_all;


/*DefaultLogger.silly = (...params) => {
    //console.log(util.getFormattedTime() + ' binance silly: ' + util.safeStringify(params));
};*/
DefaultLogger.debug = (...params) => {
    util.log('ws debug: ' + util.safeStringify(params));
};
DefaultLogger.notice = (...params) => {
    util.log('ws notice: ' + util.safeStringify(params));
};
DefaultLogger.info = (...params) => {
    //params: ["Completed keep alive cycle for listenKey(jC9zkGCwhYyL3PEzwaABpm3D3KzjAhWFrXbtcI18LENwZk8oDRaVLllnW1vivCUY) in market(usdm)",{"category":"binance-ws","listenKey":"jC9zkGCwhYyL3PEzwaABpm3D3KzjAhWFrXbtcI18LENwZk8oDRaVLllnW1vivCUY"}]
    //util.log('binance info: ' + util.safeStringify(params));
    util.log('ws info: ' + util.safeStringify(params[1]));
};
DefaultLogger.warning = (...params) => {
    util.log('ws warning: ' + util.safeStringify(params));
};
//params: ["Failed to send WS ping",{"category":"binance-ws","wsKey":"usdm_userData__GD8D3DL8StW0VeDWJYTH8a6oGLQh2BeYziU8TGmRiasj3PhppHjD3PE74ZBgxILh","exception":{}}]
DefaultLogger.error = (...params) => {
    util.log('ws error: ' + util.safeStringify(params));
    if (/failed/i.test(params[1])) {
        util.log('ws error: "failed" found');
        if (params[2].wsKey) {
            Object.entries(ws_clients).forEach(([key, value]) => {
                if (ws_clients[key].ws_key === params[2].wsKey) {
                    let apikey = key;
                    util.log(ws_clients[apikey].subscriber_id + '>> reconnecting ws after error ' + apikey.substring(0, 10));
                    ws_clients_errors[ws_clients[apikey].subscriber_id] = (ws_clients_errors[ws_clients[apikey].subscriber_id])?ws_clients_errors[ws_clients[apikey].subscriber_id]+1:1;
                    util.log(subscriber_id + '>> ws_clients_errors:' + ws_clients_errors[subscriber_id]);
                    setTimeout(connect_ws, 1000, ws_clients[apikey].subscriber_id, ws_clients[apikey].binance_acc);
                    //await connect_ws(ws_clients[apikey].subscriber_id, ws_clients[apikey].binance_acc);
                }
            });
        }
    }
};


async function save_trade_state_all() {
    util.log('save_trade_state_all...');

    for (let subscriber_id in trade_state) {
        await save_trade_state(subscriber_id);
    }
}
module.exports.save_trade_state_all = save_trade_state_all;


async function load_trade_state() {
    util.log('load_trade_state...');

    bina.trade_log(0, 'start_bot', {});

    let db_query = {
        name: 'bina.f_get_trade_state',
        text: 'SELECT * FROM bina.f_get_trade_state() as (subscriber_id integer, state json)',
        values: []
    };

    let res;
    try {
        res = await pool.query(db_query);
        for (let i=0; i<res.rows.length; i++) {
            let subscriber_id = res.rows[i].subscriber_id;
            trade_state[subscriber_id] = res.rows[i].state;
            util.log(subscriber_id + '>> loaded trade state: ' + util.safeStringify(res.rows[i].state));
        }
    } catch (err) {
        util.error('error in load_trade_state ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.load_trade_state = load_trade_state;


async function apply_trade_state_after_load() {
    util.log('apply_trade_state_after_load...');

    for (let subscriber_id in trade_state) {
        // if after start bot sees active trading lets check if it should cancel all
        if (trade_state[subscriber_id].trading && !trade_state[subscriber_id].initial.opened) {
            let now = new Date();
            let now_dt = Math.floor(now.getTime() / 1000);

            if (trade_state[subscriber_id].cancel_dt > now_dt) {
                //cancel with timeout
                let cancel_new_order_sec = trade_state[subscriber_id].cancel_dt - now_dt;
                util.log(subscriber_id + '>> initial_order_timeout_after_start waiting seconds:' + cancel_new_order_sec);
                await util.sleep(cancel_new_order_sec * 1000);
            }
            //positions have not yet opened (after cancel_new_order_sec) so lets cancel them
            if (!trade_state[subscriber_id].initial.opened) {
                bina.trade_log(subscriber_id, 'end trading', {
                    symbol: trade_state[subscriber_id].event.symbol,
                    error: 'initial_order_timeout_after_start'
                });
                trade_state[subscriber_id].trading = false;
                await cancelActiveOrders(trade_state[subscriber_id].event, 'initial_order_timeout_after_start');
                setTimeout(actualize_state, 1000, trade_state[subscriber_id].event);
            }
        }
    }

}
module.exports.apply_trade_state_after_load = apply_trade_state_after_load;


async function process_trade_events() {
    let db_query = {
        name: 'bina.f_get_trade_events',
        text: 'SELECT bina.f_get_trade_events() atj_events',
        values: []
    };

    let res;
    try {
        res = await pool.query(db_query);
        let events = res.rows[0].atj_events;

        if (events) {
            for (let i = 0; i < events.length; i++) {
                util.log(events[i].subscriber_id + '>> event ' + events[i].trade_event + ' side:' + events[i].side + ' ' + events[i].symbol + ' ' + events[i].event_date
                    + ' position_side:' + trade_state[events[i].subscriber_id].position.side + ' trading:' + trade_state[events[i].subscriber_id].trading);

                if (trade_state[events[i].subscriber_id].position.side === events[i].side) {

                    if (events[i].trade_event === 'close_position') {
                        bina.trade_log(events[i].subscriber_id, 'end trading', {symbol: trade_state[events[i].subscriber_id].symbol, reason: 'close position by timeout'});
                        trade_state[events[i].subscriber_id].trading = false;
                        await bina.closeActivePosition(events[i].subscriber_id, events[i].binance_acc,'timeout');
                        await cancelActiveOrders(events[i], 'close_position_event');
                        setTimeout(actualize_state,1000, events[i]);
                        notify_event(events[i], 'n' + events[i].acc_num + ': close by market order', 'created_order');

                    } else if (events[i].trade_event === 'take_profit_move_to_zero') {
                        await take_profit_move_to_zero(trade_state[events[i].subscriber_id].event);

                    }
                }
            }
        }
    } catch (err) {
        util.error('error in process_trade_events ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.process_trade_events = process_trade_events;


async function take_profit_move_to_zero(event) {
    try {
        let take_profit_pc = 0.1, /*zero_point_short_pc, */take_profit_price;

        if (!trade_state[event.subscriber_id].position.amount) {
            bina.trade_log(event.subscriber_id,'failed trade state - no position amount', event);
            // seems no websocket connection
            return;
        }

        //lets cancel prev order if exists
        await cancel_order(trade_state[event.subscriber_id].event, 'take_profit');

        let pos = await bina.getPosition(trade_state[event.subscriber_id].event.symbol, trade_state[event.subscriber_id].event.binance_acc);
        bina.trade_log(event.subscriber_id,'position before take_profit_move_to_zero', pos[0]);

        if (pos[0] && Number(pos[0].positionAmt) !== 0) {
            if (event.side === 'SELL') {
                /*zero_point_short_pc = trade_state[event.subscriber_id].event.config.short.zero_point_short_pc;
                if (zero_point_short_pc || zero_point_short_pc === 0) {
                    take_profit_pc = zero_point_short_pc;
                }*/
                take_profit_price = trade_state[event.subscriber_id].position.entry_price * (1 - take_profit_pc / 100);
            } else {
                /*zero_point_short_pc = trade_state[event.subscriber_id].event.config.long.zero_point_long_pc;
                if (zero_point_short_pc || zero_point_short_pc === 0) {
                    take_profit_pc = zero_point_short_pc;
                }*/
                take_profit_price = trade_state[event.subscriber_id].position.entry_price * (1 + take_profit_pc / 100);
            }
            take_profit_price = bina.roundTickSize(take_profit_price, trade_state[event.subscriber_id].event.exchange_info.filters[0].tickSize);
            take_profit_price = bina.trimToDecimalPlaces(take_profit_price, trade_state[event.subscriber_id].event.exchange_info.price_precision);

            bina.trade_log(event.subscriber_id, 'take_profit move to zero order', {
                symbol: event.symbol,
                side: trade_state[event.subscriber_id].event.close_side,
                take_profit_pc: take_profit_pc,
                take_profit_price: take_profit_price,
                current_price: event.price,
                order_quantity: trade_state[event.subscriber_id].position.amount
            });
            let order = await bina.orderLimit(event.subscriber_id, trade_state[event.subscriber_id].event.close_side, event.symbol, null, trade_state[event.subscriber_id].position.amount, event.price, take_profit_price, true, event.binance_acc, trade_state[event.subscriber_id].event.exchange_info, 'take_profit.move_to_zero');
            bina.trade_log(event.subscriber_id, 'take_profit move to zero order result', order);
            if (order && order.orderId) {
                trade_state[event.subscriber_id].take_profit.order_id = order.orderId;
                notify_event(event, 'n' + event.acc_num + ': take_profit move to zero created #' + order.orderId + ': ' + order.side + ' ' + order.origQty + ' ' + order.price, 'created_order');
            } else {
                notify_event(event, 'n' + event.acc_num + ': take_profit move to zero failed: ' + order.message, 'failed_order');
            }
        } else {
            util.log('positionAmt=0: skip take_profit_move_to_zero');
        }

        setTimeout(actualize_state,1000, event);

    } catch(err) {
        util.error('error in take_profit_move_to_zero ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function stop_loss_move_to_zero(event) {
    try {
        let stop_loss_pc = 0.1, zero_point_short_pc, stop_loss_price, first_take_profit_amount_pc;//, trailing_stop_callback_pc;

        //lets cancel prev order if exists
        await cancel_order(trade_state[event.subscriber_id].event, 'stop_loss');

        let pos = await bina.getPosition(trade_state[event.subscriber_id].event.symbol, trade_state[event.subscriber_id].event.binance_acc);
        bina.trade_log(event.subscriber_id, 'position before stop_loss_move_to_zero', pos[0]);

        if (event.side === 'SELL') {
            zero_point_short_pc = trade_state[event.subscriber_id].event.config.short.zero_point_short_pc;
            if (zero_point_short_pc || zero_point_short_pc === 0) {
                stop_loss_pc = zero_point_short_pc;
            }
            stop_loss_price = trade_state[event.subscriber_id].position.entry_price * (1 - stop_loss_pc / 100);
            first_take_profit_amount_pc = event.config.short.first_take_profit_short_amount_pc;
            //trailing_stop_callback_pc = trade_state[event.subscriber_id].event.config.short.trailing_stop_short_callback_pc;
        } else {
            zero_point_short_pc = trade_state[event.subscriber_id].event.config.long.zero_point_long_pc;
            if (zero_point_short_pc || zero_point_short_pc === 0) {
                stop_loss_pc = zero_point_short_pc;
            }
            stop_loss_price = trade_state[event.subscriber_id].position.entry_price * (1 + stop_loss_pc / 100);
            first_take_profit_amount_pc = event.config.long.first_take_profit_long_amount_pc;
            //trailing_stop_callback_pc = trade_state[event.subscriber_id].event.config.long.trailing_stop_long_callback_pc;
        }
        stop_loss_price = bina.roundTickSize(stop_loss_price, trade_state[event.subscriber_id].event.exchange_info.filters[0].tickSize);
        stop_loss_price = bina.trimToDecimalPlaces(stop_loss_price, trade_state[event.subscriber_id].event.exchange_info.price_precision);

        bina.trade_log(event.subscriber_id, 'stop_loss move to zero order', {
            symbol: event.symbol,
            side: trade_state[event.subscriber_id].event.close_side,
            stop_loss_pc: stop_loss_pc,
            stop_loss_price: stop_loss_price,
            current_price: event.price,
            order_quantity: trade_state[event.subscriber_id].position.amount
        });
        let order = await bina.orderStopMarket(event.subscriber_id, event.close_side, event.symbol, trade_state[event.subscriber_id].position.amount, stop_loss_price, event.binance_acc, 'stop_loss.move_to_zero');
        bina.trade_log(event.subscriber_id, 'stop_loss move to zero order result', order);
        if (order && order.orderId) {
            trade_state[event.subscriber_id].stop_loss.moved_to_zero = true;
            trade_state[event.subscriber_id].stop_loss.order_id = order.orderId;
            notify_event(event, 'n' + event.acc_num + ': stop_loss move to zero created #' + order.orderId + ': ' + order.side + ' ' + order.origQty + ' ' + order.stopPrice, 'created_order');

            //lets cancel average/third
            await cancel_order(trade_state[event.subscriber_id].event, 'average');
            await cancel_order(trade_state[event.subscriber_id].event, 'third');

            //trailing after stop loss
            if (first_take_profit_amount_pc === 100) {
                await trailing(event, 'stop_loss.move_to_zero');
            }

            if (!trade_state[event.subscriber_id].trading) {
                await util.sleep(2000);
                await cancelActiveOrders(event, 'trading_finished_during_stop_loss_creation');
            }

            setTimeout(actualize_state,1000, event);
        } else {
            notify_event(event, 'n' + event.acc_num + ': stop_loss move to zero failed: ' + order.message, 'failed_order');
        }

    } catch(err) {
        util.error('error in stop_loss_move_to_zero ' + err.message + ' ' + util.safeStringify(err.stack));
    }
    trade_state[event.subscriber_id].stop_loss.moving_to_zero = false;
}

async function trailing(event, order_type) {
    try {
        let trailing_stop_callback_pc;
        if (event.side === 'SELL') {
            trailing_stop_callback_pc = trade_state[event.subscriber_id].event.config.short.trailing_stop_short_callback_pc;
        } else {
            trailing_stop_callback_pc = trade_state[event.subscriber_id].event.config.long.trailing_stop_long_callback_pc;
        }

        if (trailing_stop_callback_pc > 0 && trade_state[event.subscriber_id].trading) {
            await cancel_order(trade_state[event.subscriber_id].event, 'take_profit');
            let order = await bina.orderTrailingStop(event.subscriber_id, event.close_side, event.symbol, trade_state[event.subscriber_id].position.amount, trailing_stop_callback_pc, null, event.binance_acc, 'take_profit.trailing');

            bina.trade_log(event.subscriber_id, 'trailing_order by ' + order_type + ' order result', order);
            if (order && order.orderId) {
                trade_state[event.subscriber_id].take_profit.order_id = order.orderId;
                trade_state[event.subscriber_id].take_profit.canceled = false;
                save_trade_state(event.subscriber_id);
                notify_event(trade_state[event.subscriber_id].event, 'n' + trade_state[event.subscriber_id].event.acc_num + ': trailing by ' + order_type + ' created #' + order.orderId + ': ' + order.side + ' ' + order.origQty + ' ' + order.price, 'created_order');
            } else {
                notify_event(trade_state[event.subscriber_id].event, 'n' + trade_state[event.subscriber_id].event.acc_num + ': trailing by ' + order_type + ' failed: ' + order.message, 'failed_order');
            }
        }
    } catch(err) {
        util.error('error in trailing ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}


function get_count_active_trades() {
    let count_active_trades = 0;
    for (let i=0; i<trade_state.length; i++) {
        if (trade_state[i] && trade_state[i].trading) {
            count_active_trades++;
        }
    }
    return count_active_trades;
}
module.exports.get_count_active_trades = get_count_active_trades;

/*
let ws_prices;
async function ws_prices_init(){

    ws_prices = await new WebsocketClient({
        api_key: conf.binance_core_price_acc.apikey,
        api_secret: conf.binance_core_price_acc.secretkey
    });

    ws_prices.on('open', (data) => {
        util.log('ws_prices_init opened wsKey:' + data.wsKey);
    });
    ws_prices.on('message', async (data) => {
            //console.log(data[0]);
            //await bina.saveRates1sv2(0, data);
            await bina.saveRates1sv3(1, data);
            if (conf.trading) {
                await process1s();
            }
        }
    )
    ws_prices.on('error', (data) => {
        util.error('ws_prices_init error: ' + util.safeStringify(data.error));
        ws_prices = undefined;
        setTimeout(ws_prices_init, 1000);
    });

    await ws_prices.subscribeAllMarketMarkPrice('usdm', 1000);
}
module.exports.ws_prices_init = ws_prices_init;*/


const MAX_PRICE_BUFFER_SIZE = 15;
let price_buffer = [];
let price_symbols = {}; // list of unique symbols for deduplicate
async function priceCallbackBuffered(price) {
    if (price && price.s) {
        // if duplicated price in buffer or buffer filled
        if (price_symbols[price.s] || price_buffer.length >= MAX_PRICE_BUFFER_SIZE) {
            let price_buffer_to_save = price_buffer;
            price_buffer = [];
            price_symbols = {};
            //util.log('buffer saving...' + price_buffer_to_save.length /*util.safeStringify(price_symbols)*/);

            await bina.saveRates1sv5(price_buffer_to_save);
            if (conf.trading) {
                await process1s();
            }
        }
        price_symbols[price.s] = 1;
        price_buffer.push({s:price.s, p:price.k.c, e:price["E"]});
    }
}

/*async function priceCallback(price) {
    await bina.saveRates1sv4(price);
    if (conf.trading) {
        await process1s();
    }
}*/

async function get_price_symbols() {
    try {
        //console.log('get_price_symbols');

        let db_query = {
            name: 'bina.f_get_price_symbols',
            text: "SELECT bina.f_get_price_symbols() as symbols",
            values: []
        };

        let res = await pool.query(db_query);
        //util.log('get_price_symbols: ' + res.rows[0].symbols);
        return res.rows[0].symbols;
    } catch (err) {
        util.error('get_price_symbols failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

let symbols_list = [];
async function ws_prices_init_v2() {
    const client = new Binance().options({
        APIKEY: conf.binance_core_price_acc.apikey,
        APISECRET: conf.binance_core_price_acc.secretkey
    });

    let all_symbols_list = await get_price_symbols();
    let new_symbols_list = all_symbols_list.filter(n => !symbols_list.includes(n));
    symbols_list = all_symbols_list;
    util.log('ws_prices_init_v2: new_symbols_list: ' + new_symbols_list);

    //await client.futuresBookTickerStream(priceCallback);
    //await client.futuresCandlesticks(symbols_list, '1m', priceCallback);
    //await client.futuresCandlesticks(symbols_list, '1m', priceCallbackBuffered);
    if (new_symbols_list.length > 0) {
        await client.futuresCandlesticks(new_symbols_list, '1m', priceCallbackBuffered);
    }
}
module.exports.ws_prices_init_v2 = ws_prices_init_v2;

