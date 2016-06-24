var _ = require('underscore');
var pool = [];

module.exports = {
    add(msg, callback) {
        pool.push({
            msg,
            callback
        });
    },

    next(count, callback) {
        var items = pool.splice(0, count);

        callback(_.map(items, x => x.msg));
        
        _.each(items, x => x.callback());
    }
};
