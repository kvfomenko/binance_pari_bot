"use strict";
let logger;

function init_logger(options) {
    logger = require('simple-node-logger').createRollingFileLogger(options);
}
module.exports.init_logger = init_logger;


function safeStringify(obj, indent) {
    let retVal, cache = [];
    if (typeof obj === "object") {
        retVal = JSON.stringify(
            obj,
            (key, value) =>
                typeof value === "object" && value !== null
                    ? cache.includes(value)
                        ? undefined // Duplicate reference found, discard key
                        : cache.push(value) && value // Store value in our collection
                    : value,
            0//indent
        );
        cache = null;
    } else {
        retVal = obj;
    }
    return retVal;
}
module.exports.safeStringify = safeStringify;

function getFormattedTime() {
    let today = new Date();
    let h = today.getHours().toString().padStart(2, '0');
    let m = today.getMinutes().toString().padStart(2, '0');
    let s = today.getSeconds().toString().padStart(2, '0');
    let ms = today.getMilliseconds().toString().padStart(3, '0');
    return h + ":" + m + ":" + s + "." + ms;
}
module.exports.getFormattedTime = getFormattedTime;

function getFormattedDateTime() {
    let today = new Date();
    let y = today.getFullYear();
    let mn = ("0" + (today.getMonth() + 1)).slice(-2);
    let d = ("0" + today.getDate()).slice(-2);
    let h = today.getHours().toString().padStart(2, '0');
    let m = today.getMinutes().toString().padStart(2, '0');
    let s = today.getSeconds().toString().padStart(2, '0');
    let ms = today.getMilliseconds().toString().padStart(3, '0');

    return y + '-' + mn + '-' + d + ' ' + h + ":" + m + ":" + s + '.' + ms;
}
module.exports.getFormattedDateTime = getFormattedDateTime;

function getFormattedTime() {
    let today = new Date();
    let h = today.getHours().toString().padStart(2, '0');
    let m = today.getMinutes().toString().padStart(2, '0');
    let s = today.getSeconds().toString().padStart(2, '0');
    return h + ":" + m + ":" + s;
}
module.exports.getFormattedTime = getFormattedTime;

function formatDateTime(dt) {
    let today = dt;
    let y = today.getFullYear();
    let mn = ("0" + (today.getMonth() + 1)).slice(-2);
    let d = ("0" + today.getDate()).slice(-2);
    let h = today.getHours().toString().padStart(2, '0');
    let m = today.getMinutes().toString().padStart(2, '0');
    let s = today.getSeconds().toString().padStart(2, '0');
    return y + '-' + mn + '-' + d + ' ' + h + ":" + m + ":" + s;
}
module.exports.formatDateTime = formatDateTime;

function log(val) {
    console.log(getFormattedTime() + ' ' + val);
    if (logger) {
        logger.info(val);
    }
}
function error(val) {
    console.error(getFormattedTime() + ' ' + val);
    //logger.Error(val);
    if (logger) {
        logger.info(val);
    }
}


module.exports.log = log;
module.exports.error = error;
/*module.exports.core_log = core_log;
module.exports.core_error = core_error;*/

exports.includeConfig = function(pathToConfig) {
    console.log('in util.includeConfig ' + __dirname + '/' + pathToConfig);
    try {
        var conf = require(pathToConfig);
    } catch (err) {
        conf = {};
        console.warn('NOTICE: ' + __dirname + '/' + pathToConfig + ' not found at ' + err.message + ' ' + safeStringify(err.stack));
    }
    let pathToLocalConfig = pathToConfig.replace('.json', '_local.json');
    try {
        let conf_local = require(pathToLocalConfig);
        conf = Object.assign(conf, conf_local);
    } catch (err) {
        console.warn('NOTICE: '+pathToLocalConfig+' not found at ' + err.message + ' ' + safeStringify(err.stack));
    }
    return conf;
}

exports.sleep = function(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


function fixTgMarkup(msg) {
    let regexp = /`.+?`/ig;
    if (msg) {
        let markups = Array.from(msg.matchAll(regexp));
        for (let i = 0; i < markups.length; i++) {
            msg = msg.replace(markups[i][0], '$' + i + '$');
        }
        msg = msg.replaceAll('_', '\\_');
        for (let i = 0; i < markups.length; i++) {
            msg = msg.replace('$' + i + '$', markups[i][0]);
        }
    }
    return msg;
}
module.exports.fixTgMarkup = fixTgMarkup;

function getOrderType(clientOrderId) {
    let order_type;
    if (clientOrderId && clientOrderId.indexOf('.') > 0) {
        order_type = clientOrderId.substring(0, clientOrderId.indexOf('.'));
    }
    if (order_type === 'initial' || order_type === 'average' || order_type === 'third' || order_type === 'take_profit' || order_type === 'stop_loss') {
        return order_type;
    } else {
        return 'undefined_type';
    }
}
module.exports.getOrderType = getOrderType;

function getOrderSubType(clientOrderId) {
    let order_sub_type = '';
    if (clientOrderId && clientOrderId.indexOf('.') > 0 && clientOrderId.indexOf('.', clientOrderId.indexOf('.')+1) > 0) {
        order_sub_type = clientOrderId.substring(clientOrderId.indexOf('.')+1, clientOrderId.indexOf('.', clientOrderId.indexOf('.')+1));
    }
    return order_sub_type;
}
module.exports.getOrderSubType = getOrderSubType;

//unix_time in ms
function format_date_time(unix_time) {
    const date = new Date(unix_time);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');

    return year + '-' + month + '-' + day + '_' + hours + ':' + minutes;
}
module.exports.format_date_time = format_date_time;