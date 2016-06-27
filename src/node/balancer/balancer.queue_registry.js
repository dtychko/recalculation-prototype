const _ = require('underscore');

const callbacks = {};
var queueNames = [];

function raise(event, data) {
    _.each(callbacks[event], fn => {
        fn(data);
    });
}

module.exports = {
    getAll() {
        return _.clone(queueNames);
    },

    add(queueName) {
        if (queueNames.indexOf(queueName) === -1) {
            queueNames.push(queueName);
            raise('add', queueName);
        }
    },

    remove(queueName) {
        var index = queueNames.indexOf(queueName);
        if (index !== -1) {
            queueNames = queueNames.splice(index, 1);
            raise('remove', queueName);
        }
    },

    on(event, callback) {
        callbacks[event] = callbacks[event] || [];
        callbacks[event].push(callback);
    }
};
