const _ = require('underscore');
const ProtoBuf = require('protobufjs');
const amqp = require('amqplib');
const Guid = require('guid');
const fetch = require('node-fetch');
const {log, error} = require('./../logging/loggin');
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
        log(`App Started`);
    })
    .catch(err => {
        error(err);
    });

function createChannel() {
    return amqp.connect(RABBIT_SERVER_URL)
        .then(conn => conn.createChannel())
        .then(ch => {
            log(`1. Connected and created channel.`);
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
        log(`2. Asserted queues/exchanges.`);
        return ch;
    });
}

function startListening(ch) {
    return Promise.all([
        ch.consume(QUEUE_DISCOVERY_QUEUE, logErrors(queueDiscoveryMessageConsumer(ch))),
        ch.consume(METRIC_SETUP_QUEUE, logErrors(metricSetupQueueConsumer(ch)))
    ]).then(() => {
        log(`3. Initialized consumers.`);
        return ch;
    });
}

function queueDiscoveryMessageConsumer(ch) {
    return msg => {
        log(`Received RequestQueuesCommand.`);

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

                log(`Processed RequestQueuesCommand`, `Response=${JSON.stringify(queueCollection)}`);
            })
            .catch(err => {
                ch.noAck(msg);

                error(err);
            });
    };
}

function metricSetupQueueConsumer(ch) {
    return msg => {
        const event = MetricSetupChangedEvent.decode(msg.content);

        const eventId = generateEventId();
        const accountId = event.accountId;
        const metricSetup = event.metricSetup;
        const queueName = getQueueName(accountId);

        log(`Received MetricSetupChangedEvent`,
            `EventId=${eventId}`,
            `AccountId=${accountId}`,
            `MetricSetupId=${metricSetup.id}`);

        accountStorage.addIfNotExists(accountId)
            .then(notifyIfQueueAdded(ch, queueName))
            .then(() => getTargets(metricSetup))
            .then(targetIds => processTargetIds(ch, queueName, eventId, accountId, metricSetup, targetIds))
            .then(() => {
                ch.ack(msg);

                log(`Processed MetricSetupChangedEvent`,
                    `EventId=${eventId}`);
            })
            .catch(err => {
                ch.nack(msg);

                error(err);
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

                log(`Sent QueueChangedEvent`,
                    `QueueName=${queueName}`,
                    `Modification=${1}`);
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
        log(`Received CancellationMap.`,
            `Data=${JSON.stringify(json)}`);

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

            log(`Sent ComputationCancelledEvent`,
                `EventId=${eventId}`,
                `AccountId=${accountId}`,
                `MetricSetupId=${metricSetupId}`,
                `Targets=${targetIds}`);
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
            var commandId = generateCommandId();
            var command = new CalculateMetricCommand({
                commandId,
                eventId,
                accountId,
                metricSetup,
                targetIds: batches[i]
            });

            var responseFlag = ch.sendToQueue(queueName, command.toBuffer());

            log(`Sent CalculateMetricCommand`,
                `CommandId=${commandId}`,
                `EventId=${eventId}`,
                `AccountId=${accountId}`,
                `MetricSetupId=${metricSetup.id}`,
                `Targets=${batches[i]}`);
        }

        res();
    });
}

function getQueueName(accountId) {
    return `calc_requests_for_account_${accountId}`;
}

var commandCounter = 0;
var eventCounter = 0;

function generateCommandId() {
    //return Guid.raw();
    return `cmd#${++commandCounter}`;
}

function generateEventId() {
    //return Guid.raw();
    return `event#${++eventCounter}`;
}

var entityCountMap = {
    'bug': {
        startFrom: 1,
        count: 5
    },
    'userstory': {
        startFrom: 1000,
        count: 2
    },
    'feature': {
        startFrom: 1000000,
        count: 1
    }
};

function getTargets(metricSetup) {
    return new Promise(resolve => {
        setTimeout(() => {
            var entityTypes = metricSetup.entityTypes.toLowerCase().replace(' ', '').split(',');
            var result = entityTypes.reduce((acc, type) => {
                const description = entityCountMap[type] || {startFrom: 0, count: 0};
                const startFrom = description.startFrom;
                var count = description.count;

                return acc.concat(_.range(startFrom, startFrom + count));
            }, []);

            resolve(result);
        }, 1000);
    });
}

function createBatches(targetIds) {
    var result = [];
    var batch = [];

    for (var i = 0; i < targetIds.length; i++) {
        batch.push(targetIds[i]);

        if (batch.length >= 2) {
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
            error(err);
        }
    };
}
