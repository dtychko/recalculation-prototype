var redis = require('redis');
var db = redis.createClient('redis://192.168.99.100:6379/1');
db.on('error', err => {
    console.error(' [Error] Redis error', err);
});

return;

var startTime = new Date();

single()
    .then(acc => {
        var delta = new Date() - startTime;
        console.log(' [x] single ->', acc.length, delta);
    })
    .then(() => {
        startTime = new Date();
        return multi()
    })
    .then(acc => {
        var delta = new Date() - startTime;
        console.log(' [x] batch ->', acc.length, delta);
        process.exit();
    });

function single() {
    var promise = Promise.resolve([]);

    for (let i = 0; i < 1000; i++) {
        promise = promise.then(acc =>
            new Promise(res => {
                db.get(`1_1_${i}`, (err, eventId) => {
                    acc.push(eventId);
                    res(acc);
                })
            }));
    }

    return promise;
}

function multi() {
    return new Promise(res => {
        const keys = [];

        for (let i = 0; i < 1000; i++) {
            keys.push(`1_1_${i}`);
        }

        db.mget(keys, (err, replies) => {
            res(replies);
        });
    });
}
