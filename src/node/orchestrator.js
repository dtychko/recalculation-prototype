const _ = require('underscore');
const ProtoBuf = require('protobufjs');
const amqp = require('amqplib');
const Guid = require('guid');
const fetch = require('node-fetch');
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
var CANCELLATION_QUEUE = 'cancellation_queue';
var QUEUE_PREFETCH = 1;

amqp.connect(RABBIT_SERVER_URL)
    .then(conn => conn.createChannel())
    .then(ch => {
        console.log(' [1] Created channel.');
        return ch;
    })
    .then(ch =>
        Promise.all([
            ch.assertQueue(METRIC_SETUP_QUEUE, {durable: false}),
            ch.assertQueue(QUEUE_DISCOVERY_QUEUE, {durable: false}),
            ch.assertExchange(QUEUE_CHANGED_EXCHANGE, 'fanout', {durable: false}),
            ch.prefetch(QUEUE_PREFETCH)
        ]).then(() => {
            console.log(' [2] Asserted queues/exchanges.');
        }).then(() =>
            Promise.all([
                ch.consume(QUEUE_DISCOVERY_QUEUE, withLogging(queueDiscoveryMessageConsumer(ch))),
                ch.consume(METRIC_SETUP_QUEUE, withLogging(metricSetupQueueConsumer(ch)))
            ]).then(() => {
                console.log(' [3] Initialized consumers.');
            })
        ))
    .then(() => {
        console.log(' [x] App started')
    });

function metricSetupQueueConsumer(ch) {
    return msg => {
        const event = MetricSetupChangedEvent.decode(msg.content);

        console.log(' [x] Received MetricSetupChangedEvent ', JSON.stringify(event));

        const eventId = Guid.raw();
        const accountId = event.accountId;
        const metricSetup = event.metricSetup;
        const queueName = getQueueName(accountId);

        accountStorage.addIfNotExists(accountId)
            .then(added => {
                if (added) {
                    return ch.assertQueue(queueName, {durable: false})
                        .then(() => {
                            var addQueueMessage = new QueueChangedEvent({
                                queueName: queueName,
                                modification: 1
                            });
                            ch.publish(QUEUE_CHANGED_EXCHANGE, '', addQueueMessage.toBuffer());
                        });
                }
            })
            .then(() => getTargets(accountId, metricSetup))
            .then(targetIds =>
                updateComputationLog(eventId, accountId, metricSetup.id, targetIds)
                    .then(cancellationMap => sendMessagesToCancellationQueue(accountId, metricSetup, ch, cancellationMap))
                    .then(() => sendCalculationBatches(ch, queueName, eventId, accountId, metricSetup, targetIds))
            )
            .then(() => {
                ch.ack(msg);
                console.log(' [x] Processed MetricSetupChangedEvent.');
            }, err => {
                console.error(err);
            });
    };
}

function sendCalculationBatches(ch, queueName, eventId, accountId, metricSetup, targetIds) {
    return new Promise(res => {
        var batches = createBatches(targetIds);

        for (let i = 0; i < batches.length; i++) {
            var command = new CalculateMetricCommand({
                commandId: Guid.raw(),
                eventId: eventId,
                accountId: accountId,
                metricSetup: metricSetup,
                targetIds: targetIds
            });

            var responseFlag = ch.sendToQueue(queueName, command.toBuffer());
            command.targetIds = [];
            console.log(' [x] Sent ', JSON.stringify(command));
        }
        res();
    });
}

function sendMessagesToCancellationQueue(accountId, metricSetup, ch, cancellationMap) {
    return new Promise(res => {
        var cancellationGroups = _.reduce(cancellationMap, (acc, eId, tId) => {
            acc[eId] = acc[eId] || [];
            acc[eId].push(tId);
            return acc;
        }, {});

        _.each(cancellationGroups, (tIds, eId) => {
            var computationCancelledEvent = new ComputationCancelledEvent({
                accountId: accountId,
                metricSetupId: metricSetup.id,
                targetIds: _.map(tIds, x => parseInt(x, 10)),
                eventId: eId
            });

            var responseFlag = ch.sendToQueue(CANCELLATION_QUEUE, computationCancelledEvent.toBuffer());
            console.log(` [x] Sent cancellation command for event ${eId}`);
        });

        console.log(' [x] Sent all cancellation commands.');

        res();
    });
}

function queueDiscoveryMessageConsumer(ch) {
    return msg => {
        console.log(` [x] Received RequestQueuesCommand.`);
        var content = msg.content;
        var command = RequestQueuesCommand.decode(content);

        accountStorage.getAccounts()
            .then(accountIds => {
                var queueCollection = new QueuesCollection({
                    queueNames: _.map(accountIds, item => getQueueName(item))
                });

                //TODO: investigate processing of false response from sendToQueue
                var responseFlag = ch.sendToQueue(
                    msg.properties.replyTo,
                    queueCollection.toBuffer(),
                    {correlationId: msg.properties.correlationId}
                );

                ch.ack(msg);
                console.log(` [x] Processed RequestQueuesCommand.`);
            });
    };
}

function getQueueName(accountId) {
    return `calc_requests_for_account_${accountId}`;
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
            console.log(res.status, res.statusText);
            return Promise.reject('Error during updating computation log');
        }

        return res.json();
    }, err => {
        console.log(` [-] Update computation log error '${err}'`);
    }).then(json => {
        console.log(` [x] Received cancellation map.`, JSON.stringify(json).substring(0, 80) + '...');
        return json;
    });
}

var entityCountMap = {
    'bug': 1000,
    'userstory': 100,
    'feature': 10
};

function getTargets(accountId, metricSetup) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            var entityTypes = metricSetup.entityTypes.toLowerCase().replace(' ', '').split(',');
            var count = _.reduce(entityTypes, (acc, type) => {
                return acc + (entityCountMap[type] || 0);
            }, 0);
            var result = _.range(0, count);

            resolve(result)
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

function withLogging(fn) {
    return (...args) => {
        try {
            fn(...args);
        } catch (e) {
            console.error(e);
        }
    };
}
