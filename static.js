const StaticServer = require('static-server');
const util = require('./modules/util');

const conf = util.includeConfig('../app_conf.json');

util.init_logger({
    errorEventName: 'error',
    logDirectory: './logs', // NOTE: folder must exist and be writable...
    fileNamePattern: 'static-<DATE>.log',
    dateFormat: 'YYYY.MM.DD'
});


let server = new StaticServer({
    rootPath: conf.http.root_path,            // required, the root of the server file tree
    port: conf.http.port,               // required, the port to listen
    name: 'Pari-bot-http-server',   // optional, will set "X-Powered-by" HTTP header
    cors: '*',                // optional, defaults to undefined
    templates: {
        index: 'index.html',      // optional, defaults to 'index.html'
    }
});

server.start(function () {
    util.log('Server listening to ' + server.port);
});

server.on('request', function (req, res) {
    let ip =  req.headers['x-forwarded-for'] ||
        req.connection.remoteAddress ||
        req.socket.remoteAddress ||
        (req.connection.socket ? req.connection.socket.remoteAddress : null);

    util.log(ip + ' ' + req.path);
    // req.path is the URL resource (file name) from server.rootPath
    // req.elapsedTime returns a string of the request's elapsed time
});

process.on('uncaughtException', function (err) {
    util.log('uncaughtException:' + err.message);
    util.log(err.stack);
    process.exit(1);
})
