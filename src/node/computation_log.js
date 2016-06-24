var express = require('express');
var bodyParser = require('body-parser');
var redis = require('redis');
var app = express();

var db = redis.createClient('redis://192.168.99.100:6379/1');
db.on('error', (err) => {
    console.log(` [-] Redis error '${err}'`);
});

app.use(bodyParser.json());

app.post('/update', (req, res) => {
    console.log(` [x] Received POST '/update'. Payload=${JSON.stringify(req.body)}`);

    var eventId = req.body.eventId;
    var accountId = req.body.accountId;
    var metricSetupId = req.body.metricSetupId;
    var targetIds = req.body.targetIds;

    if (!eventId || !accountId || !metricSetupId || !targetIds || !targetIds.length) {
        res.status(400).send('Bad Request');
        return;
    }

    var generateKey = makeKeyGenerator(accountId, metricSetupId);
    var promise = Promise.resolve({});

    for (var i = 0; i < targetIds.length; i++) {
        let targetId = targetIds[i];
        let key = generateKey(targetId);

        promise = promise.then(map =>
            new Promise((res, rej) => {
                db.get(key, (err, eventId) => {
                    if (err) {
                        console.log(` [-] Redis get error '${err}'`);
                        rej();
                    }

                    if (eventId) {
                        map[targetId] = eventId;
                    }

                    res(map);
                });
            })
        );
    }

    promise.then(map => {
        for (var i = 0; i < targetIds.length; i++) {
            let key = generateKey(targetIds[i]);

            db.set(key, eventId, err => {
                if (err) {
                    console.log(` [-] Redis set error '${err}'`);
                }
            });
        }

        res.json(map);
        console.log(` [x] Updated eventId '${eventId}'`);
    }, () => {
        res.status(500).send('Internal Server Error');
        console.log(` [-] Error during updating eventId '${eventId}'`);
    });
});


function makeKeyGenerator(accountId, metricSetupId) {
    return targetId => `${accountId}_${metricSetupId}_${targetId}`;
}
// app.get('/getUid', (req, res) => {
//     var accountId = req.query.accountId;
//     var metricSetupId = req.query.metricSetupId;
//     var targetId = req.query.targetId;
//
//     console.log(` [x] Received GET '/getUid'. AccountId=${accountId}, MSId=${metricSetupId}, TargetId=${targetId}`);
//    
//     if (!accountId || !metricSetupId || !targetId) {
//         res.status(400).send('Bad Request');
//         return;
//     }
//
//     var key = `${accountId}_${metricSetupId}_${targetId}`;
//
//     db.get(key, (err, uid) => {
//         if (err) {
//             console.log(` [-] Redis get error '${err}'`);
//             return;
//         }
//
//         var result = uid || null;
//         res.end(result);
//
//         console.log(` [x] Sent ${result}`);
//     });
// });
//
// app.post('/updateUid', (req, res) => {
//     console.log(` [x] Received POST '/updateUid'. Payload=${JSON.stringify(req.body)}`);
//
//     var uid = req.body.uid;
//     var accountId = req.body.accountId;
//     var metricSetupId = req.body.metricSetupId;
//     var targetIds = req.body.targetIds;
//
//     if (!uid || !accountId || !metricSetupId || !targetIds || !targetIds.length) {
//         res.status(400).send('Bad Request');
//         return;
//     }
//
//     for (var i = 0; i < targetIds.length; i++) {
//         var key = `${accountId}_${metricSetupId}_${targetIds[i]}`;
//         db.set(key, uid, (err) => {
//             if (err) {
//                 console.log(` [-] Redis set error '${err}'`);
//             }
//         });
//
//         console.log(` [x] Added/Updated uid '${uid}' for key '${key}'`);
//     }
//
//     res.end();
// });

app.listen(8081, () => {
    console.log(` [x] App started`);
});
