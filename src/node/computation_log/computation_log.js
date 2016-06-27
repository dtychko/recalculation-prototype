const express = require('express');
const bodyParser = require('body-parser');
const redis = require('redis');
const dateFormat = require('dateformat');
const app = express();

const db = redis.createClient('redis://192.168.99.100:6379/1');
db.on('error', err => {
    console.error(` [${now()}] ERROR Redis error`, err);
});

app.use(bodyParser.json());

app.post('/update', (req, res) => {
    var eventId = req.body.eventId;
    var accountId = req.body.accountId;
    var metricSetupId = req.body.metricSetupId;
    var targetIds = req.body.targetIds;

    if (!eventId || !accountId || !metricSetupId || !targetIds || !targetIds.length) {
        console.log(` [${now()}] Bad Request POST '/update' Body=${JSON.stringify(req.body)}`);
        res.status(400).send('Bad Request');
        return;
    }

    console.log(` [${now()}] Received POST '/update'. EventId=${eventId}, AccountId=${accountId}, MetricSetupId=${metricSetupId}, TargetCount=${targetIds.length}`);

    update(eventId, accountId, metricSetupId, targetIds)
        .then(result => {
            res.json(result);
            console.log(` [${now()}] Processed POST '/update'. Response=${JSON.stringify(result).substring(0, 80)}...`);
        })
        .catch(err => {
            console.error(` [${now()}] ERROR`, err);
            res.status(500).send('Internal Server Error');
        });
});

function update(eventId, accountId, metricSetupId, targetIds) {
    var generateKey = makeKeyGenerator(accountId, metricSetupId);

    var multi = db.multi();
    var getPromise = getLastEventIds(multi, targetIds, generateKey);
    var setPromise = setEventId(multi, targetIds, generateKey, eventId);

    return execMulti(multi)
        .then(() => Promise.all([getPromise, setPromise]))
        .then(([lastEvents]) => lastEvents);
}

function getLastEventIds(client, targetIds, generateKey) {
    return new Promise((resolve, reject) => {
        const keys = targetIds.map(x => generateKey(x));

        client.mget(keys, (err, eventIds) => {
            if (err) {
                reject(err);
                return;
            }

            var result = targetIds
                .map((targetId, index) => ({
                    targetId,
                    eventId: eventIds[index]
                }))
                .filter(pair => pair.eventId);

            resolve(result);
        })
    });
}

function setEventId(client, targetIds, generateKey, eventId) {
    return new Promise((resolve, reject) => {
        var args = targetIds
            .reduce((acc, targetId) => acc.concat(generateKey(targetId), eventId), []);

        client.mset(args, (err, replies) => {
            if (err) {
                reject(err);
                return;
            }

            resolve();
        })
    });
}

function execMulti(multi) {
    return new Promise((resolve, reject) => {
        multi.exec(err => {
            if (err) {
                reject(err);
                return;
            }

            resolve();
        });
    });
}

function makeKeyGenerator(accountId, metricSetupId) {
    return targetId => `${accountId}_${metricSetupId}_${targetId}`;
}

function now() {
    return dateFormat(new Date(), 'HH:MM:ss.L');
}

app.listen(8081, () => {
    console.log(` [${now()}] App started`);
});
