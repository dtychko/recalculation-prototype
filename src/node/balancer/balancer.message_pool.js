var _ = require('underscore');
var pool = [];
var consumers = [];

function add(msg, onConsumed) {
    if (consumers.length) {
        var consumer = consumers.shift();
        consumer(consumerPayload(msg, onConsumed));
        return;
    }

    pool.push({
        msg,
        onConsumed
    });
}

function next() {
    if (pool.length) {
        var next = pool.shift();
        return Promise.resolve(consumerPayload(next.msg, next.onConsumed));
    }

    return new Promise(resolve => {
        consumers.push(resolve);
    });
}

function consumerPayload(msg, onConsumed) {
    return {
        msg: msg,
        ack: onConsumed,
        nack: _.partial(add, msg, onConsumed)
    };
}

module.exports = {
    add,
    next
};
