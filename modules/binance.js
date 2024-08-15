const { USDMClient } = require('binance');
//const { WebsocketClient } = require('binance');
const util = require('./util');
const {Pool} = require('pg');
//const Binance = require("node-binance-api");

const conf = util.includeConfig('../app_conf.json');

function init_logger(options) {
   util.init_logger(options);
}
module.exports.init_logger = init_logger;

let trade_logger;
function init_trade_logger(options) {
    trade_logger = require('simple-node-logger').createRollingFileLogger(options);
}
module.exports.init_trade_logger = init_trade_logger;

util.log('binance.js connecting to db...');
const pool = new Pool({
    connectionString: conf.connectionDB,
    max: 3,
    min: 1,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 3000
});
const pool_prices = new Pool({
    connectionString: conf.connectionDB,
    max: 1,
    min: 1,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 3000
});

let clients = {};
let core_client = new USDMClient({
    api_key: conf.binance_core_price_acc.apikey,
    api_secret: conf.binance_core_price_acc.secretkey
});


/*let core_clients = [];
for (let acc in conf.binance_core_accounts) {
    core_clients[core_clients.length] = new USDMClient({
        api_key: acc.apikey,
        api_secret: acc.secretkey,
    });
}*/

function fixRounding(val) {
    let val_str = val.toString();
    //console.log(val_str);

    if (/999/.test(val_str)) {
        //console.log('found');
        let dot_pos = val_str.indexOf('.');
        let len = val_str.length;
        let len_after_dot = len - dot_pos + 1;
        //console.log('dot_pos:' + dot_pos + ' len:' + len + ' len_after_dot:' + len_after_dot);
        let end = val_str.substring(len - 1);
        //console.log('ending_digit ' + end);
        let numadd = 10 - Number(end);
        let addon = '0.'.padEnd(len_after_dot - 1, '0') + numadd;
        //console.log(addon);
        let val_fixed = val + Number(addon);
        //console.log(val_fixed);
        return val_fixed;
    } else {
        return val;
    }
}
module.exports.fixRounding = fixRounding;


function trimToDecimalPlaces(number, precision) {
    if (number) {
        let array = number.toString().split('.');
        if (!array[1]) {
            array.push('0');
        }
        array.push(array.pop().substring(0, precision));
        const trimmedstr = array.join('.');
        return parseFloat(trimmedstr);
    } else {
        util.log('trimToDecimalPlaces error: no value');
        return 0;
    }
}
module.exports.trimToDecimalPlaces = trimToDecimalPlaces;

//precision examples: 0.1, 0.01, 0.00001
function roundTickSize(number, tick_size) {
    let result = number - number % tick_size;
    result = fixRounding(result);
    return result;
}
module.exports.roundTickSize = roundTickSize;

async function getClient(binance_acc, force_reconnect) {
    let client, apikey, secretkey;
    if (binance_acc) {
        apikey = binance_acc.apikey;
        secretkey = binance_acc.secretkey;
    }

    if (!apikey) {
        util.error('No apikey');
        return;
    }

    if (!clients[apikey] || force_reconnect) {
        if (!secretkey) {
            util.error('No secretkey');
            return;
        }

        if (apikey && secretkey) {
            util.log('connecting to Binance... ' + apikey.substring(0,10));
            client = await new USDMClient({
                api_key: apikey,
                api_secret: secretkey,
                beautify: false
            });
            util.log('connected...' );
            clients[apikey] = client;
        }
    }

    return clients[apikey];
}
module.exports.getClient = getClient;


/**
 * Get USDT balance
 */
async function getBalance(symbol, binance_acc) {
    try {
        let client = await getClient(binance_acc);
        if (!client) {
            util.error('no client connection');
            return 'API is not configured';
        }

        let allBalances = await client.getBalance();
        if (!symbol) {
            symbol = 'USDT';
        }
        const bal = allBalances.find((elem) => elem.asset === symbol);
        if (bal) {
            //util.log('balance: ' + bal.asset + ' ' + bal.availableBalance + ' ' + unixTimeFormat(bal.updateTime));
            return bal.availableBalance;
        } else {
            util.log('balance: ' + symbol);
            return null;
        }
    } catch (err) {
        util.error('Error: balance request failed: ' + binance_acc.apikey.substring(0,10) + ' ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.getBalance = getBalance;


/**
 * Get all non-zero balances in all accounts
 */
async function getBalances(binance_acc) {
    try {
        let client = await getClient(binance_acc);
        if (!client) {
            util.error('no client connection');
            return 'Api is not configured';
        }

        let allBalances = await client.getBalance();
        let result = '';
        allBalances.forEach(function (elem, ind) {
            if (allBalances[ind].asset === 'USDT') {
                result += allBalances[ind].asset + ': ' + Math.floor(allBalances[ind].availableBalance *100) /100 + '\n';
            }
        })

        return result;
    } catch (err) {
        util.error('Error: balance request failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.getBalances = getBalances;


/**
 * Get last asset price
 */
async function getPrice(symbol, binance_acc) {
    try {
        let client = await getClient(binance_acc);
        if (!client) {
            util.error('no client connection');
            return 'api is not configured';
        }

        const prices = (await client.getSymbolPriceTicker({symbol: symbol}));
        //util.log('getPrice: ' + symbol + ' ' + prices.price);
        return prices.price;

    } catch (err) {
        util.error('getPrice failed: ' + err.message + ' ' + util.safeStringify(err.stack));
        return 'failed';
    }
}
module.exports.getPrice = getPrice;

/**
 * Get list of all pairs with prices
 */
async function getAllPrices(binance_acc) {
    try {
        let client = await getClient(binance_acc);
        if (!client) {
            util.error('no client connection');
            return 'Api is not configured';
        }

        const prices = await client.getSymbolPriceTicker();
        return prices;
    } catch (err) {
        util.error('getAllPrices failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.getAllPrices = getAllPrices;


async function orderMarket(subscriber_id, side, symbol, usdtAmount, quantityAmount, current_price, reduce_only, binance_acc, exchange_info, order_type) {
    try {
        side = side.toUpperCase();
        let client = await getClient(binance_acc);
        if (!client) {
            util.error(subscriber_id + '>> error: api is not configured');
            return {message: 'error: api is not configured'};
        }

        let usdt, quantity;
        if (usdtAmount) {
            usdt = usdtAmount;
            if (usdt < conf.min_order_usdt) {
                util.log(subscriber_id + '>> warning, usdt less them min_order_usdt ' + usdt + '<' + conf.min_order_usdt + ', usdt->' + conf.min_order_usdt);
                usdt = conf.min_order_usdt;
            }
            quantity = usdt / current_price;
        }

        if (quantityAmount) {
            quantity = trimToDecimalPlaces(quantityAmount, exchange_info.quantity_precision);
        } else {
            quantity = trimToDecimalPlaces(quantity, exchange_info.quantity_precision);
            usdt = quantity * price;
            if (quantity === 0) {
                util.log(subscriber_id + '>> error, quantity = 0, ' + usdt + ' ' + current_price + ' ' + quantity + ' ' + exchange_info.quantity_precision);
                return {message: 'error: quantity = 0'};
            }
            if (usdt < conf.min_order_usdt) {
                util.log(subscriber_id + '>> error: usdt less them min_order_usdt ' + usdt + '<' + conf.min_order_usdt);
                return {message: 'error: usdt less them min_order_usdt ' + usdt + '<' + conf.min_order_usdt};
            }
        }
        let clientOrderId = order_type + '.' + (Math.random() + 1).toString().substring(2,10);

        const orderRequest = {
            newClientOrderId: clientOrderId,
            reduceOnly: reduce_only,
            positionSide: 'BOTH',
            symbol: symbol,
            quantity: quantity,
            side: side,
            type: 'MARKET',
            newOrderRespType: 'FULL',
        };
        util.log(subscriber_id + '>> submitting orderMarket: ' + JSON.stringify(orderRequest));
        let orderResult = await client.submitNewOrder(orderRequest);

        return orderResult;
    } catch (err) {
        util.error(subscriber_id + '>> orderMarket failed: ' + err.message + ' ' + util.safeStringify(err.stack));
        return err;
    }
}
module.exports.orderMarket = orderMarket;


async function orderLimit(subscriber_id, side, symbol, usdtAmount, quantityAmount, current_price, limit_price, reduce_only, binance_acc, exchange_info, order_type) {
    try {
        side = side.toUpperCase();
        let client = await getClient(binance_acc);
        if (!client) {
            util.error(subscriber_id + '>> error: api is not configured');
            return {message: 'error: api is not configured'};
        }

        if (!limit_price || limit_price === 0) {
            util.log(subscriber_id + '>> error: empty limit_price');
            return {message: 'error: empty limit_price'};
        }

        let usdt, quantity;
        if (usdtAmount) {
            usdt = usdtAmount;
            if (usdt < conf.min_order_usdt) {
                util.log(subscriber_id + '>> warning, usdt less them min_order_usdt ' + usdt + '<' + conf.min_order_usdt + ', usdt->' + conf.min_order_usdt);
                usdt = conf.min_order_usdt;
            }
            quantity = usdt / limit_price;
        }
        if (quantityAmount) {
            quantity = trimToDecimalPlaces(quantityAmount, exchange_info.quantity_precision);
        } else {
            quantity = trimToDecimalPlaces(quantity, exchange_info.quantity_precision);
            usdt = quantity * limit_price;
            if (quantity === 0) {
                util.log(subscriber_id + '>> error, quantity = 0, ' + usdt + ' ' + limit_price + ' ' + quantity + ' ' + exchange_info.quantity_precision);
                return {message: 'error: quantity = 0'};
            }
            if (usdt < conf.min_order_usdt) {
                util.log(subscriber_id + '>> error: usdt less them min_order_usdt ' + usdt + '<' + conf.min_order_usdt);
                return {message: 'error: usdt less them min_order_usdt ' + usdt + '<' + conf.min_order_usdt};
            }
        }
        let clientOrderId = order_type + '.' + (Math.random() + 1).toString().substring(2,10);

        const orderRequest = {
            newClientOrderId: clientOrderId,
            reduceOnly: reduce_only,
            positionSide: 'BOTH',
            symbol: symbol,
            quantity: quantity,
            price: limit_price,
            timeInForce: 'GTC',
            side: side,
            type: 'LIMIT',
            newOrderRespType: 'FULL',
        };
        util.log(subscriber_id + '>> submitting orderLimit: ' + JSON.stringify(orderRequest));
        let orderResult = await client.submitNewOrder(orderRequest);

        return orderResult;
    } catch (err) {
        util.error(subscriber_id + '>> orderLimit failed: ' + err.message + ' ' + util.safeStringify(err.stack));
        return err;
    }
}
module.exports.orderLimit = orderLimit;


async function orderStopMarket(subscriber_id, side, symbol, quantityAmount, stop_price, binance_acc, order_type) {
    try {
        side = side.toUpperCase();
        let client = await getClient(binance_acc);
        if (!client) {
            util.error(subscriber_id + '>> error: api is not configured');
            return {message: 'error: api is not configured'};
        }

        if (!stop_price || stop_price === 0) {
            util.log(subscriber_id + '>> error: empty stop_price');
            return {message: 'error: empty stop_price'};
        }
        let clientOrderId = order_type + '.' + (Math.random() + 1).toString().substring(2,10);

        const orderRequest = {
            newClientOrderId: clientOrderId,
            reduceOnly: true,
            positionSide: 'BOTH',
            symbol: symbol,
            quantity: quantityAmount,
            stopPrice: stop_price,
            side: side,
            type: 'STOP_MARKET',
            newOrderRespType: 'FULL',
        };
        util.log(subscriber_id + '>> submitting orderStopMarket: ' + JSON.stringify(orderRequest));
        let orderResult = await client.submitNewOrder(orderRequest);

        return orderResult;
    } catch (err) {
        util.error(subscriber_id + '>> orderStopMarket failed: ' + err.code + ' ' + err.message + ' ' + util.safeStringify(err.stack));
        return err;
    }
}
module.exports.orderStopMarket = orderStopMarket;


async function orderTrailingStop(subscriber_id, side, symbol, quantityAmount, callbackRate, activation_price, binance_acc, order_type) {
    try {
        side = side.toUpperCase();
        let client = await getClient(binance_acc);
        if (!client) {
            util.error(subscriber_id + '>> error: api is not configured');
            return {message: 'error: api is not configured'};
        }

        let clientOrderId = order_type + '.' + (Math.random() + 1).toString().substring(2,10);

        const orderRequest = {
            newClientOrderId: clientOrderId,
            symbol: symbol,
            type: 'TRAILING_STOP_MARKET',
            side: side,
            positionSide: 'BOTH',
            quantity: quantityAmount,
            reduceOnly: true,
            activatePrice: activation_price,
            callbackRate: callbackRate,
            workingType: "CONTRACT_PRICE",
            priceProtect: true,
            newOrderRespType: 'FULL'
        };
        console.log('Submitting order:', orderRequest);
        let orderResult = await client.submitNewOrder(orderRequest);
        console.log('Order response:', JSON.stringify(orderResult, null, 2));

        return orderResult;
    } catch (e) {
        console.error('Order failed', e);
    }
}
module.exports.orderTrailingStop = orderTrailingStop;


async function getPosition(symbol, binance_acc){
    try {
        let client = await getClient(binance_acc);
        if (!client) {
            util.error('no client connection');
            return 'Api is not configured';
        }

        let position = await client.getPositions({symbol: symbol});
        //util.log(`Position:`, JSON.stringify(position, null, 2));

        return position;
    } catch (err) {
        util.error('getPosition failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.getPosition = getPosition;


async function getActivePos(binance_acc){
    try {
        let client = await getClient(binance_acc);
        if (!client) {
            util.error('no client connection');
            return;
        }

        let positions = await client.getPositions();
        let active_positions = [];
        positions.forEach(function(elem, ind) {
            if (elem.positionAmt > 0) {
                elem.side = 'BUY';
                active_positions.push(elem);
            } else if (elem.positionAmt < 0) {
                elem.side = 'SELL';
                active_positions.push(elem);
            }
        });

        return active_positions;
    } catch (err) {
        util.error('getActivePos failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.getActivePos = getActivePos;

async function getAllPos(binance_acc){
    try {
        let client = await getClient(binance_acc);
        if (!client) {
            util.error('no client connection');
            return;
        }

        return await client.getPositions();
    } catch (err) {
        util.error('getAllPos failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}


async function getActivePositions(binance_acc) {
    try {
        let positions = await getActivePos(binance_acc);

        util.log('Active positions:');
        let answer = '';
        if (positions && positions.length > 0) {
            positions.forEach(function (elem, ind) {
                if (elem.positionAmt > 0 || elem.positionAmt < 0) {
                    util.log(positions[ind].symbol + ': ' + positions[ind].positionAmt + ' entry:' + positions[ind].entryPrice);
                    //active_positions[active_positions.length] = positions[ind];
                    positions[ind].unRealizedProfit = trimToDecimalPlaces(positions[ind].unRealizedProfit, 3);
                    answer += positions[ind].symbol + ': ' + positions[ind].positionAmt + ' entry:' + positions[ind].entryPrice + ' unRealizedProfit:' + positions[ind].unRealizedProfit + '\n';
                    //answer += '  `/stop n' + acc_num + ' ' + positions[ind].symbol + '`\n';
                }
            });
        }
        if (!answer) {
            answer = 'No active positions\n\n';
        } else {
            answer = 'Active positions:\n' + answer;
        }

        return answer;
    } catch (err) {
        util.error('getActivePositions failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.getActivePositions = getActivePositions;


async function closeActivePosition(subscriber_id, binance_acc, order_type){
    try {
        let client = await getClient(binance_acc);
        if (!client) {
            util.error(subscriber_id + '>> no client connection');
            return 'error: api is not configured';
        }

        let positions = await client.getPositions();
        //util.log(subscriber_id + '>> active positions: ' + JSON.stringify(positions, null, 2));

        for (const elem of positions) {
            if (elem.positionAmt > 0 || elem.positionAmt < 0) {
                //util.log(subscriber_id + '>> close position ' + elem.symbol + ': ' + elem.positionAmt + ' entry_price:' + elem.entryPrice);

                let orderRequest, side;
                if (elem.positionAmt < 0) {
                    side = 'BUY';
                } else {
                    side = 'SELL';
                }
                let clientOrderId = order_type + '.close.' + (Math.random() + 1).toString().substring(2,10);

                orderRequest = {
                    newClientOrderId: clientOrderId,
                    reduceOnly: true,
                    positionSide: 'BOTH',
                    symbol: elem.symbol,
                    quantity: Math.abs(Number(elem.positionAmt)),
                    side: side,
                    type: 'MARKET',
                    newOrderRespType: 'FULL',
                };

                util.log(subscriber_id + '>> submitting close position by market for:' + JSON.stringify(orderRequest));
                let orderResult = await client.submitNewOrder(orderRequest);
                util.log(subscriber_id + '>> close position result: ' + util.safeStringify(orderResult));

                return orderResult;
            }
        }

    } catch (err) {
        util.error(subscriber_id + '>> closeActivePosition failed: ' + err.code + ' ' + err.message + ' ' + util.safeStringify(err.stack));
        return err;
    }
}
module.exports.closeActivePosition = closeActivePosition;


async function getChart(symbol, from, to){
    try {
        let db_query = {
            name: 'bina.f_get_chart',
            text: 'SELECT FROM bina.f_get_chart($1, $2, $3) aj_chart',
            //text: 'SELECT bina.f_get_chart($1, $2, $3) aj_chart',
            values: [symbol, from, to]
        };
        //console.log(db_query);

        let res;
        try {
            res = await pool.query(db_query);
        } catch (err) {
            util.log('catch error in getChart ' + err.message + ' ' + util.safeStringify(err.stack));
        }

        return res.rows[0].aj_chart;
    } catch (err) {
        util.error('getChart failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.getChart = getChart;

async function getChart1s(telegram_id, acc_num, symbol, from, to){
    try {
        let db_query = {
            name: 'bina.f_get_chart1s',
            text: 'SELECT aj_chart, av_template FROM bina.f_get_chart1s($1, $2, $3, $4, $5)',
            values: [telegram_id, acc_num, symbol, from, to]
        };
        //console.log(db_query);

        let res;
        try {
            res = await pool.query(db_query);
            return res.rows[0];
        } catch (err) {
            util.log('catch error in getChart1s ' + err.message + ' ' + util.safeStringify(err.stack));
        }

    } catch (err) {
        util.error('getChart1s failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.getChart1s = getChart1s;


async function getActiveOrd(binance_acc){
    try {
        let client = await getClient(binance_acc);
        if (!client) {
            util.error('no client connection');
            return;
        }

        let orders = await client.getAllOpenOrders();

        return orders;
    } catch (err) {
        util.error('getActiveOrd failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.getActiveOrd = getActiveOrd;

async function getActiveOrders(acc_num, binance_acc){
    try {
        let orders = await getActiveOrd(binance_acc);

        //util.log('Active orders:');
        let answer = '';
        if (orders && orders.length > 0) {
            orders.forEach(function (elem, ind) {
                let order_type = util.getOrderType(orders[ind].clientOrderId);
                let order_sub_type = util.getOrderSubType(orders[ind].clientOrderId);
                if (!orders[ind].price && orders[ind].stopPrice !== 0) {
                    orders[ind].price = orders[ind].stopPrice;
                }
                //util.log(order_type + ' ' + orders[ind].side + ' ' + orders[ind].symbol + ' ' + orders[ind].price + ' qty:' + orders[ind].origQty + ' ' + orders[ind].orderId);
                answer += order_type + '.' + order_sub_type + ': ' + orders[ind].side + ' ' + orders[ind].origQty + ' (' + orders[ind].price + ')\n  `/cancel n' + acc_num + ' ' + orders[ind].symbol + ' ' + orders[ind].orderId + '`\n';
            });
        }
        if (!answer) {
            answer = 'No active orders\n';
        } else {
            answer = 'Active orders:\n' + answer;
        }

        return answer;
    } catch (err) {
        util.error('getActiveOrders failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.getActiveOrders = getActiveOrders;


async function cancelOrder(symbol, orderId, binance_acc){
    try {
        let client = await getClient(binance_acc);
        if (!client) {
            util.error('no client connection');
            //return 'Api is not configured';
            return false;
        }

        await client.cancelOrder({symbol:symbol, orderId:orderId});

        return true;
    } catch (err) {
        util.error('cancelOrder failed for order ' + orderId + ' ' + err.message + ' ' + util.safeStringify(err.stack));
        return false;
    }
}
module.exports.cancelOrder = cancelOrder;


async function setLeverage(symbol, leverage, binance_acc){
    try {
        let client = await getClient(binance_acc);
        if (!client) {
            util.error('no client connection');
            return 'Api is not configured';
        }

        await client.setLeverage({symbol:symbol, leverage:leverage});
        let answer = symbol + ' leverage updated to ' + leverage;

        return answer;
    } catch (err) {
        util.error('setLeverage failed for leverage:' + leverage + ' symbol:' + symbol + ' ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.setLeverage = setLeverage;


////////////////////////// core functions
/*async function getPrices(core_client_id) {
    try {
        return await core_clients[core_client_id].getSymbolPriceTicker();
    } catch (err) {
        util.error('getPrices failed: ' + core_client_id + ' ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}*/

/*async function getExchangeInfo(core_client_id) {
    try {
        let res = await core_clients[core_client_id].getExchangeInfo();
        return res.symbols;
    } catch (err) {
        util.error('getExchangeInfo failed: ' + core_client_id + ' ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}*/

/*async function saveRates() {
    try {
        console.log('in saveRates');
        let prices = await getPrices();

        let db_query = {
            name: 'bina.f_save_rate',
            text: 'SELECT bina.f_save_rate($1::json[])',
            values: [prices]
        };

        try {
            let res = await pool_prices.query(db_query);
            //console.log('db.success: ' + JSON.stringify(res.rows[0].aj_response));
        } catch (err) {
            util.log('catch error in saveRates ' + err.message + ' ' + util.safeStringify(err.stack));
        }

    } catch (err) {
        util.error('saveRates failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.saveRates = saveRates;*/


/*async function saveRates1s(core_client_id) {
    try {
        let pd = new Date();
        //console.log(util.getFormattedTime() + ': before getPrices ' + core_client_id);
        let prices = await getPrices(core_client_id);
        //util.log('prices: ' + util.safeStringify(prices));
        let ms = new Date() - pd;
        //console.log('getPrices ' + core_client_id + ' in ' + ms + 'ms');

        let db_query = {
            name: 'bina.f_save_rate1s',
            text: "SELECT TO_CHAR(bina.f_save_rate1s($1::json[], $2, $3), 'YYYY-MM-DD HH24:MI:SS') as ad_cur_price_date",
            values: [prices, core_client_id, ms]
        };

        try {
            let res = await pool_prices.query(db_query);
            //util.log('ad_cur_price_date: ' + res.rows[0].ad_cur_price_date);
        } catch (err) {
            util.log('catch error in saveRates1s ' + err.message + ' ' + util.safeStringify(err.stack));
        }

    } catch (err) {
        util.error('saveRates1s failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.saveRates1s = saveRates1s;*/

/*async function saveRates1sv2(core_client_id, prices) {
    try {
        //console.log('getPrices ' + core_client_id + ' in ' + ms + 'ms');

        let db_query = {
            name: 'bina.f_save_rate1s',
            text: "SELECT TO_CHAR(bina.f_save_rate1s($1::json[], $2, $3), 'YYYY-MM-DD HH24:MI:SS') as ad_cur_price_date",
            values: [prices, core_client_id, null]
        };

        try {
            let res = await pool_prices.query(db_query);
            //util.log('ad_cur_price_date: ' + res.rows[0].ad_cur_price_date);
        } catch (err) {
            util.log('catch error in saveRates1s ' + err.message + ' ' + util.safeStringify(err.stack));
        }

    } catch (err) {
        util.error('saveRates1s failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.saveRates1sv2 = saveRates1sv2;*/

/*async function saveRates1sv3(core_client_id, prices) {
    try {
        //console.log('saveRates1sv3 ' + core_client_id + ' in ' + ms + 'ms');

        let db_query = {
            name: 'bina.f_save_rate1sv3',
            text: "SELECT TO_CHAR(bina.f_save_rate1sv3($1::json[], $2, $3), 'YYYY-MM-DD HH24:MI:SS') as ad_cur_price_date",
            values: [prices, core_client_id, null]
        };

        try {
            let res = await pool_prices.query(db_query);
            //util.log('ad_cur_price_date: ' + res.rows[0].ad_cur_price_date);
        } catch (err) {
            util.log('catch error in saveRates1sv3 ' + err.message + ' ' + util.safeStringify(err.stack));
        }

    } catch (err) {
        util.error('saveRates1sv3 failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.saveRates1sv3 = saveRates1sv3;*/

/*async function saveRates1sv4(prices) {
    try {
        //console.log('saveRates1sv4 ' + core_client_id + ' in ' + ms + 'ms');

        let db_query = {
            name: 'bina.f_save_rate1sv4',
            text: "SELECT TO_CHAR(bina.f_save_rate1sv4($1::json), 'YYYY-MM-DD HH24:MI:SS') as ad_cur_price_date",
            values: [prices]
        };

        try {
            let res = await pool_prices.query(db_query);
            //util.log('ad_cur_price_date: ' + res.rows[0].ad_cur_price_date);
        } catch (err) {
            util.log('catch error in saveRates1sv4 ' + err.message + ' ' + util.safeStringify(err.stack));
        }

    } catch (err) {
        util.error('saveRates1sv4 failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.saveRates1sv4 = saveRates1sv4;*/

async function saveRates1sv5(prices) {
    try {
        //console.log('saveRates1sv5 ' + core_client_id + ' in ' + ms + 'ms');

        let db_query = {
            name: 'bina.f_save_rate1s',
            text: "SELECT TO_CHAR(bina.f_save_rate1s($1::json[]), 'YYYY-MM-DD HH24:MI:SS') as ad_cur_price_date",
            values: [prices]
        };

        try {
            let res = await pool_prices.query(db_query);
            //util.log('ad_cur_price_date: ' + res.rows[0].ad_cur_price_date);
        } catch (err) {
            util.log('catch error in saveRates1sv5 ' + err.message + ' ' + util.safeStringify(err.stack));
        }

    } catch (err) {
        util.error('saveRates1sv5 failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.saveRates1sv5 = saveRates1sv5;

async function save_state(subscriber_id, balance_usdt, active_orders, positions) {
    let db_query = {
        name: 'bina.f_save_state',
        text: 'SELECT bina.f_save_state($1, $2, $3, $4)',
        values: [subscriber_id, balance_usdt, active_orders, positions]
    };

    try {
        await pool.query(db_query);
        //util.log(subscriber_id + '>> an_binance_leverage: ' + res.rows[0].an_binance_leverage);
    } catch (err) {
        util.error('error in save_state ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function trade_log(subscriber_id, comment, params) {
    let par;
    if (typeof params === "object") {
        par = params;
        if (par && par.binance_acc) {
            delete par.binance_acc;
            //par.binance_acc = null;
        }
    } else {
        par = {params: params};
    }

    util.log(subscriber_id + '>> ' + comment + ' ' + util.safeStringify(par));

    let db_query = {
        name: 'bina.f_trade_log',
        text: 'SELECT bina.f_trade_log($1, $2, $3) as av_event',
        values: [subscriber_id, comment, par]
    };

    trade_logger.info("INSERT INTO bina.trade_log (log_date, subscriber_id, comment, params) VALUES ('"+util.getFormattedDateTime()+"', "+((subscriber_id >= 0)?subscriber_id:'null')+", '"+comment+"', '"+util.safeStringify(par)+"');");

    try {
        let res = await pool.query(db_query);
        return res.rows[0].av_event;
    } catch (err) {
        util.error('error in trade_log ' + err.message + ' ' + util.safeStringify(err.stack));
        //util.log("SELECT bina.f_trade_log("+subscriber_id+", '"+comment+"', '"+util.safeStringify(par)+"');");
    }

}
module.exports.trade_log = trade_log;

async function get_balance(event) {
    let balance_usdt = await getBalance('USDT', event.binance_acc);
    util.log(event.subscriber_id + '>> get balance ' + util.safeStringify({balance_usdt:balance_usdt}));
    //await trade_log(event.subscriber_id, 'get balance', {balance_usdt:balance_usdt});
    return balance_usdt;
}
module.exports.get_balance = get_balance;

async function get_active_pos(event) {
    let pos = await getActivePos(event.binance_acc);
    let obj = Object.assign({}, pos);
    util.log(event.subscriber_id + '>> get active pos ' + util.safeStringify(obj));
    //await trade_log(event.subscriber_id, 'get active pos', obj);
    return pos;
}
module.exports.get_active_pos = get_active_pos;

async function get_all_pos(event) {
    let pos = await getAllPos(event.binance_acc);
    util.log(event.subscriber_id + '>> get all pos ' + util.safeStringify({positions: (pos)?pos.length:0}));
    //await trade_log(event.subscriber_id, 'get all pos', {positions: (pos)?pos.length:0});
    return pos;
}
module.exports.get_all_pos = get_all_pos;

async function get_active_orders(event) {
    let ord = await getActiveOrd(event.binance_acc);
    let obj = Object.assign({}, ord);
    util.log(event.subscriber_id + '>> get active orders ' + util.safeStringify(obj));
    //await trade_log(event.subscriber_id, 'get active orders', obj);
    return ord;
}
module.exports.get_active_orders = get_active_orders;

async function actualize_state(acc) {
    util.log(acc.subscriber_id + '>> actualize_state');
    try {
        let balance_usdt = await get_balance(acc);
        let orders = await get_active_orders(acc);
        let positions = await get_all_pos(acc);
        await save_state(acc.subscriber_id, balance_usdt, (orders)?orders.length:0, positions);

        let active_positions = [];
        if (positions) {
            positions.forEach(function (elem, ind) {
                if (elem.positionAmt > 0 || elem.positionAmt < 0) {
                    active_positions.push(elem);
                    util.log(acc.subscriber_id + '>> active_position: ' + util.safeStringify(elem));
                }
            });
        }
        //util.log(acc.subscriber_id + '>> actualize_state ' + util.safeStringify(active_positions));

        if (acc.set_binance_leverage_symbol_list) {
            for (let i = 0; i < acc.set_binance_leverage_symbol_list.length && i < 20; i++) {
                util.log(acc.subscriber_id + '>> ' + i + ' set leverage ' + acc.set_binance_leverage_symbol_list[i] + ' => ' + acc.binance_leverage);
                await setLeverage(acc.set_binance_leverage_symbol_list[i], acc.binance_leverage, acc.binance_acc);
                await util.sleep(5 * 1000);
            }
        }

        if (orders && orders.length > 0 || active_positions.length > 0) {
            return true;
        } else {
            return false;
        }
    } catch (err) {
        util.error('error in actualize_state ' + acc.binance_acc.apikey.substring(0,10) + ' ' + err.message + ' ' + util.safeStringify(err.stack));
    }
    return false;
}
module.exports.actualize_state = actualize_state;

async function getExchangeInfo() {
    try {
        let res = await core_client.getExchangeInfo();
        return res.symbols;
    } catch (err) {
        util.error('getExchangeInfo failed: ' + core_client_id + ' ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}

async function saveExchangeInfo() {
    try {
        let exchange_info = await getExchangeInfo();

        let db_query = {
            name: 'bina.f_save_exchange_info',
            text: 'SELECT bina.f_save_exchange_info($1::json[])',
            values: [exchange_info]
        };

        try {
            let res = await pool.query(db_query);
            //console.log('db.success: ' + JSON.stringify(res.rows[0].aj_response));
        } catch (err) {
            util.log('catch error in saveExchangeInfo ' + err.message + ' ' + util.safeStringify(err.stack));
        }

    } catch (err) {
        util.error('saveExchangeInfo failed: ' + err.message + ' ' + util.safeStringify(err.stack));
    }
}
module.exports.saveExchangeInfo = saveExchangeInfo;