var dateFormat = require('dateformat');

function now() {
    return dateFormat(new Date(), 'HH:MM:ss.L');
}

function logWith(log, msg, ...params) {
    log(` [${now()}]`, msg);

    for (var i = 0; i < params.length; i++) {
        log(`    `, params[i].replace(/\n/g, '\n    '));
    }
}

function log(msg, ...params) {
    logWith(console.log, msg, ...params);
}

function error(msg, ...params) {
    logWith(console.error, 'ERROR ' + msg, ...params);
}

module.exports = {
    log,
    error
};
