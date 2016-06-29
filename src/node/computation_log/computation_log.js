const express = require('express');
const bodyParser = require('body-parser');
const redis = require('redis');
const {log, error} = require('./../logging/loggin');
const amqp = require('amqplib');
const ProtoBuf = require('protobufjs');

const app = express();
const builder = ProtoBuf.loadProtoFile('./computation_log.proto');

const REDIS_SERVER_URL = 'redis://192.168.99.100:6379/1';
const RABBIT_SERVER_URL = 'amqp://192.168.99.100';
const PROCESSING_FINISHED_QUEUE = 'processing_finished_queue';

const ProcessingFinishedEvent = builder.build('ProcessingFinishedEvent');

const db = redis.createClient(REDIS_SERVER_URL);
db.on('error', err => {
    error(`Redis error`, err);
});

app.use(bodyParser.json());

app.post('/update', (req, res) => {
    var eventId = req.body.eventId;
    var accountId = req.body.accountId;
    var metricSetupId = req.body.metricSetupId;
    var targetIds = req.body.targetIds;

    if (!eventId || !accountId || !metricSetupId || !targetIds || !targetIds.length) {
        log(`Bad Request POST '/update'`,
            `Body=${JSON.stringify(req.body)}`);

        res.status(400).send('Bad Request');
        return;
    }

    log(`Received POST '/update'`,
        `EventId=${eventId}`,
        `AccountId=${accountId}`,
        `MetricSetupId=${metricSetupId}`,
        `Targets=${targetIds}`);

    update(eventId, accountId, metricSetupId, targetIds)
        .then(result => {
            res.json(result);

            log(`Processed POST '/update'`,
                `Response=${JSON.stringify(result)}`);
        })
        .catch(err => {
            res.status(500).send('Internal Server Error');

            error(`Internal Server Error POST '/update'`, err);
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

createChannel()
    .then(initProcessingFinishedQueue);

function createChannel() {
    return amqp.connect(RABBIT_SERVER_URL)
        .then(conn => conn.createChannel())
        .then(ch => {
            log(` 1. Connected and created channel.`);
            return ch;
        });
}

function initProcessingFinishedQueue(ch) {
    return ch.assertQueue(PROCESSING_FINISHED_QUEUE, {durable: false})
        .then(q => {
            ch.consume(q.queue, msg => {
                const event = ProcessingFinishedEvent.decode(msg.content);
                const eventId = event.eventId;
                const accountId = event.accountId;
                const metricSetupId = event.metricSetupId;
                const targetIds = event.targetIds;

                log(`Received ProcessingFinishedEvent`,
                    `EventId = ${eventId}`,
                    `AccountId = ${accountId}`,
                    `MetricSetupId = ${metricSetupId}`,
                    `TargetIds = ${targetIds}`);

                remove(eventId, accountId, metricSetupId, targetIds)
                    .then(({replies, iterator}) => {
                        log(`${replies} keys were removed on ${iterator} attempt.`,
                            `EventId = ${eventId}`,
                            `AccountId = ${accountId}`,
                            `MetricSetupId = ${metricSetupId}`,
                            `TargetIds = ${targetIds}`);
                    })
                    .catch(err => {
                        error(`Error while removing keys: ${err}`,
                            `EventId = ${eventId}`,
                            `AccountId = ${accountId}`,
                            `MetricSetupId = ${metricSetupId}`,
                            `TargetIds = ${targetIds}`);
                    })
                    .then(() => {
                        ch.ack(msg);
                    });
            })
        })
        .then(() => {
            log(` 2. Initialiazed ${PROCESSING_FINISHED_QUEUE}.`);
            return ch;
        });
}

function remove(eventId, accountId, metricSetupId, targetIds) {
    var generateKey = makeKeyGenerator(accountId, metricSetupId);

    return watchRemove(targetIds, eventId, generateKey);
}

function watchRemove(targetIds, eventId, generateKey) {
    return new Promise((resolve, reject) => {
        const keys = targetIds.map(x => generateKey(x));
        const client = new redis.createClient(REDIS_SERVER_URL);

        client.watch(keys);

        function recursiveRemoval(iterator) {
            iterator++;

            client.mget(keys, function (err, eventIds) {
                const keysToDelete = keys
                    .filter((key, index) => eventIds[index] === eventId);
                if (keysToDelete.length) {
                    const multi = client.multi();
                    multi.del(keysToDelete, (err, ok) => {
                    });
                    multi.exec((err, replies) => {
                        if (err || replies === null) {
                            if (iterator === 5) {
                                reject({err, iterator});
                                return;
                            }

                            recursiveRemoval(iterator);
                            return;
                        }
                        resolve({replies, iterator});
                    });
                } else {
                    resolve({replies: 0, iterator: iterator})
                }

            });
        }

        recursiveRemoval(0);
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

app.listen(8081, () => {
    log(`App Started`);
});
