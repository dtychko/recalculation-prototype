const _ = require('underscore');
const ProtoBuf = require('protobufjs');
const amqp = require('amqplib');
const Guid = require('guid');
const fetch = require('node-fetch');
const dateFormat = require('dateformat');
const accountStorage = require('./orchestrator.account_storage');

const builder = ProtoBuf.loadProtoFile('./orchestrator.proto');
const MetricSetupChangedEvent = builder.build('MetricSetupChangedEvent');
const CalculateMetricCommand = builder.build('CalculateMetricCommand');
const ComputationCancelledEvent = builder.build('ComputationCancelledEvent');
const RequestQueuesCommand = builder.build('RequestQueuesCommand');
const QueuesCollection = builder.build('QueuesCollection');
const QueueChangedEvent = builder.build('QueueChangedEvent');

var RABBIT_SERVER_URL = 'amqp://192.168.99.100';
var COMPUTATION_LOG_URL = 'http://127.0.0.1:8081/update';
var METRIC_SETUP_QUEUE = 'metric_setup_events';
const QUEUE_DISCOVERY_QUEUE = 'queue_discovery_queue';
const QUEUE_CHANGED_EXCHANGE = 'queue_changed_queue';
var CANCELLATION_EXCHANGE = 'cancellation_exchange';
var QUEUE_PREFETCH = 5;

createChannel()
    .then(prepareChannel)
    .then(startListening)
    .then(() => {
        console.log(` [${now()}] App started`);
    })
    .catch(err => {
        console.error(` [${now()}] ERROR`, err);
    });

function createChannel() {
    return amqp.connect(RABBIT_SERVER_URL)
        .then(conn => conn.createChannel())
        .then(ch => {
            console.log(` [${now()}] 1. Connected and created channel.`);
            return ch;
        });
}

function prepareChannel(ch) {
    return Promise.all([
        ch.assertQueue(METRIC_SETUP_QUEUE, {durable: false}),
        ch.assertQueue(QUEUE_DISCOVERY_QUEUE, {durable: false}),
        ch.assertExchange(QUEUE_CHANGED_EXCHANGE, 'fanout', {durable: false}),
        ch.assertExchange(CANCELLATION_EXCHANGE, 'fanout', {durable: false}),
        ch.prefetch(QUEUE_PREFETCH)
    ]).then(() => {
        console.log(` [${now()}] 2. Asserted queues/exchanges.`);
        return ch;
    });
}

function startListening(ch) {
    return Promise.all([
        ch.consume(QUEUE_DISCOVERY_QUEUE, logErrors(queueDiscoveryMessageConsumer(ch))),
        ch.consume(METRIC_SETUP_QUEUE, logErrors(metricSetupQueueConsumer(ch)))
    ]).then(() => {
        console.log(` [${now()}] 3. Initialized consumers.`);
        return ch;
    });
}

function queueDiscoveryMessageConsumer(ch) {
    return msg => {
        console.log(` [${now()}] Received RequestQueuesCommand.`);

        RequestQueuesCommand.decode(msg.content);

        accountStorage.getAccounts()
            .then(accountIds => {
                var queueCollection = new QueuesCollection({
                    queueNames: accountIds.map(x => getQueueName(x))
                });

                //TODO: investigate processing of false response from sendToQueue
                var responseFlag = ch.sendToQueue(
                    msg.properties.replyTo,
                    queueCollection.toBuffer(),
                    {correlationId: msg.properties.correlationId}
                );

                ch.ack(msg);

                console.log(` [${now()}] Processed RequestQueuesCommand. Response=${JSON.stringify(queueCollection)}`);
            })
            .catch(err => {
                ch.noAck(msg);

                console.error(` [${now()}] ERROR`, err);
            });
    };
}

function metricSetupQueueConsumer(ch) {
    return msg => {
        const event = MetricSetupChangedEvent.decode(msg.content);

        console.log(` [${now()}] Received MetricSetupChangedEvent.`, JSON.stringify(event).substring(0, 80));

        const eventId = Guid.raw();
        const accountId = event.accountId;
        const metricSetup = event.metricSetup;
        const queueName = getQueueName(accountId);

        accountStorage.addIfNotExists(accountId)
            .then(notifyIfQueueAdded(ch, queueName))
            .then(() => getTargets(accountId, metricSetup))
            .then(targetIds => processTargetIds(ch, queueName, eventId, accountId, metricSetup, targetIds))
            .then(() => {
                ch.ack(msg);

                console.log(` [${now()}] Processed MetricSetupChangedEvent#${eventId}.`);
            })
            .catch(err => {
                ch.nack(msg);

                console.error(` [${now()}] ERROR`, err);
            });
    };
}

function notifyIfQueueAdded(ch, queueName) {
    return added => {
        if (!added) {
            return;
        }

        return ch.assertQueue(queueName, {durable: false})
            .then(() => {
                var event = new QueueChangedEvent({
                    queueName: queueName,
                    modification: 1
                });
                ch.publish(QUEUE_CHANGED_EXCHANGE, '', event.toBuffer());

                console.log(` [${now()}] Sent QueueChangedEvent. Payload=${JSON.stringify(event)}`);
            });
    };
}

function processTargetIds(ch, queueName, eventId, accountId, metricSetup, targetIds) {
    return updateComputationLog(eventId, accountId, metricSetup.id, targetIds)
        .then(cancellationMap => sendMessagesToCancellationQueue(ch, accountId, metricSetup.id, cancellationMap))
        .then(() => sendCalculationBatches(ch, queueName, eventId, accountId, metricSetup, targetIds));
}

function updateComputationLog(eventId, accountId, metricSetupId, targetIds) {
    return fetch(COMPUTATION_LOG_URL, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            eventId,
            accountId,
            metricSetupId,
            targetIds
        })
    }).then(res => {
        if (!res.ok) {
            return Promise.reject(
                `Error during updating computation log. Status=${res.status}, StatusText=${res.statusText}`);
        }

        return res.json();
    }).then(json => {
        console.log(` [${now()}] Received cancellation map.`, JSON.stringify(json).substring(0, 80) + '...');
        return json;
    });
}

function sendMessagesToCancellationQueue(ch, accountId, metricSetupId, cancellationMap) {
    return new Promise(res => {
        var groups = cancellationMap.reduce((acc, item) => {
            acc[item.eventId] = acc[item.eventId] || [];
            acc[item.eventId].push(item.targetId);
            return acc;
        }, {});

        _.each(groups, (targetIds, eventId) => {
            var computationCancelledEvent = new ComputationCancelledEvent({
                accountId,
                metricSetupId,
                targetIds,
                eventId
            });

            var responseFlag = ch.publish(CANCELLATION_EXCHANGE, '', computationCancelledEvent.toBuffer());

            console.log(` [${now()}] Sent ComputationCancelledEvent#${eventId}`);
        });

        setTimeout(() => {
            res()
        }, 500);
        //res();
    });
}

function sendCalculationBatches(ch, queueName, eventId, accountId, metricSetup, targetIds) {
    return new Promise(res => {
        var batches = createBatches(targetIds);

        for (let i = 0; i < batches.length; i++) {
            var commandId = Guid.raw();
            var command = new CalculateMetricCommand({
                commandId,
                eventId,
                accountId,
                metricSetup,
                targetIds
            });

            var responseFlag = ch.sendToQueue(queueName, command.toBuffer());

            console.log(` [${now()}] Sent CalculateMetricCommand#${commandId}. EventId=${eventId}`);
        }

        res();
    });
}

function getQueueName(accountId) {
    return `calc_requests_for_account_${accountId}`;
}

var entityCountMap = {
    'bug': 500,
    'userstory': 200,
    'feature': 50
};

function getTargets(accountId, metricSetup) {
    return new Promise(resolve => {
        setTimeout(() => {
            var entityTypes = metricSetup.entityTypes.toLowerCase().replace(' ', '').split(',');
            var count = entityTypes.reduce((acc, type) => {
                return acc + (entityCountMap[type] || 0);
            }, 0);

            resolve(_.range(0, count))
        }, 1000);
    });
}

function createBatches(targetIds) {
    var result = [];
    var batch = [];

    for (var i = 0; i < targetIds.length; i++) {
        batch.push(targetIds[i]);

        if (batch.length >= 100) {
            result.push(batch);
            batch = [];
        }
    }

    if (batch.length !== 0) {
        result.push(batch);
    }

    return result;
}

function logErrors(fn) {
    return (...args) => {
        try {
            fn(...args);
        } catch (err) {
            console.error(` [${now()}] ERROR`, err);
        }
    };
}

function now() {
    return dateFormat(new Date(), 'HH:MM:ss.L');
}
