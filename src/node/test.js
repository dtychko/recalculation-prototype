var redis = require('redis');
var db = redis.createClient('redis://192.168.99.100:6379/2');
db.on('error', err => {
    console.error(' [Error] Redis error', err);
});

db.multi()
    .mget(['01', '02', '03'], (err, replies) => {
        console.log('mget', replies);
    })
    .mset(['01', 11, '02', 12, '03', 13], (err, replies) => {
        console.log('mset', replies);
    })
    .exec((err, replies) => {
        console.log('exec', replies);
    });
