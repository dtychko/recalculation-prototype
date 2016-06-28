const _ = require('underscore');
const ProtoBuf = require('protobufjs');
const amqp = require('amqplib');
const Guid = require('guid');
const {log, error} = require('./../logging/loggin');
const queueRegistry = require('./balancer.queue_registry');
const messagePool = require('./balancer.message_pool');
const cancellationStore = require('./balancer.cancellation_store');

const builder = ProtoBuf.loadProtoFile('./balancer.proto');
const ComputationCancelledEvent = builder.build('ComputationCancelledEvent');
const RequestQueuesCommand = builder.build('RequestQueuesCommand');
const QueuesCollection = builder.build('QueuesCollection');
const QueueChangedEvent = builder.build('QueueChangedEvent');
const CalculateMetricCommand = builder.build('CalculateMetricCommand');
const ProcessingFinishedEvent = builder.build('ProcessingFinishedEvent');

const RABBIT_SERVER_URL = 'amqp://192.168.99.100';
const QUEUE_DISCOVERY_QUEUE = 'queue_discovery_queue';
const QUEUE_CHANGED_EXCHANGE = 'queue_changed_queue';
const PROCESSING_FINISHED_QUEUE = 'processing_finished_queue';
const CANCELLATION_EXCHANGE = 'cancellation_exchange';
const SHORT_QUEUE = 'short_queue';
const CHANNEL_PREFETCH = 100;
const SHORT_QUEUE_DNO = 10;

createChannel()
    .then(prepareChannel)
    .then(initProcessingFinishedQueue)
    .then(subscribeToCancellationQueue)
    .then(subscribeToQueueChangedExchange)
    .then(discoverQueues)
    .then(initConsumers)
    .then(initShortQueue)
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
    return ch.prefetch(CHANNEL_PREFETCH)
        .then(() => {
            log(`2. Set channel prefetch size.`);
            return ch;
        });
}

function initProcessingFinishedQueue(ch) {
    return ch.assertQueue(PROCESSING_FINISHED_QUEUE, { durable: false })
        .then(() => { log(` Initialiazed ${PROCESSING_FINISHED_QUEUE}.`); })
        .then(() => ch);
}

function subscribeToCancellationQueue(ch) {
    return Promise.all([
            ch.assertExchange(CANCELLATION_EXCHANGE, 'fanout', {durable: false}),
            ch.assertQueue('', {exclusive: true, durable: false})
        ])
        .then(([ex, q]) => ch.bindQueue(q.queue, ex.exchange, '').then(() => q))
        .then(q => ch.consume(q.queue, logErrors(msg => {
            const event = ComputationCancelledEvent.decode(msg.content);

            log(`Received ComputationCancelledEvent`,
                `EventId=${event.eventId}`,
                `AccountId=${event.accountId}`,
                `MetricSetupId=${event.metricSetupId}`,
                `TargetCount=${event.targetIds.length}`);

            cancellationStore.add(event.accountId, event.metricSetupId, event.targetIds, event.eventId);
        }), {noAck: true}))
        .then(() => {
            log(`3. Subscribed to ComputationCancelledEvent.`);
            return ch;
        });
}

function subscribeToQueueChangedExchange(ch) {
    return Promise.all([
            ch.assertExchange(QUEUE_CHANGED_EXCHANGE, 'fanout', {durable: false}),
            ch.assertQueue('', {exclusive: true, durable: false})
        ])
        .then(([ex, q]) => ch.bindQueue(q.queue, ex.exchange, '').then(() => q))
        .then(q => ch.consume(q.queue, logErrors(msg => {
            const event = QueueChangedEvent.decode(msg.content);

            log(`Received QueueChangedEvent`,
                `QueueName=${event.queueName}`,
                `Modification=${event.modification}`);

            if (event.modification === 1) {
                queueRegistry.add(event.queueName);
            } else if (event.modification === 2) {
                queueRegistry.remove(event.queueName);
            }
        }), {noAck: true}))
        .then(() => {
            log(`4. Subscribed to QueueChangedEvent.`);
            return ch;
        });
}

function discoverQueues(ch) {
    return new Promise(res => {
        Promise.all([
                ch.assertQueue(QUEUE_DISCOVERY_QUEUE, {durable: false}),
                ch.assertQueue('', {exclusive: true, durable: false})
            ])
            .then(([requestQ, responseQ]) => {
                var correlationId = Guid.raw();
                var consumerTag;

                ch.consume(responseQ.queue, logErrors(msg => {
                        if (msg.properties.correlationId === correlationId) {
                            var collection = QueuesCollection.decode(msg.content);

                            _.each(collection.queueNames, q => {
                                queueRegistry.add(q);
                            });

                            ch.cancel(consumerTag);

                            log(`5. Received QueuesCollection`,
                                `QueueNames=[${_.toArray(collection.queueNames).join(', ')}]`);

                            res(ch);
                        }
                    }), {noAck: true})
                    .then(ok => {
                        consumerTag = ok.consumerTag;
                        ch.sendToQueue(requestQ.queue, new RequestQueuesCommand({}).toBuffer(), {
                            correlationId: correlationId,
                            replyTo: responseQ.queue
                        });

                        log(`5. Sent request for queue discovery.`);
                    });
            });
    });
}

function initConsumers(ch) {
    var queueNameToConsumer = {};

    function initConsumer(queueName) {
        if (queueNameToConsumer[queueName]) {
            return Promise.resolve();
        }

        queueNameToConsumer[queueName] = {};
        return ch.assertQueue(queueName, {durable: false})
            .then(() => ch.consume(queueName, logErrors(msg => {
                var command = CalculateMetricCommand.decode(msg.content);

                log(`Received CalculateMetricCommand`,
                    `EventId=${command.eventId}`,
                    `AccountId=${command.accountId}`,
                    `MetricSetupId=${command.metricSetup.id}`,
                    `TargetCount=${command.targetIds.length}`);

                messagePool.add(command, () => {
                    ch.ack(msg);
                });
            })))
            .then(ok => {
                queueNameToConsumer[queueName].consumerTag = ok.consumerTag;

                log(`Started consuming queue`,
                    `QueueName=${queueName}`);
            });
    }

    queueRegistry.on('add', initConsumer);
    // TODO: implement queueRegistry.on('remove', queueName => { ... });

    return Promise.all(_.map(queueRegistry.getAll(), initConsumer))
        .then(() => {
            log(`6. Initialized consumers.`);
            return ch;
        });
}

function initShortQueue(ch) {
    return ch.assertQueue(SHORT_QUEUE, {durable: false})
        .then(() => {
            checkShortQueue(ch);
        })
        .then(() => {
            log(`7. Initialized short queue.`);
            return ch;
        });
}

function checkShortQueue(ch) {
    ch.checkQueue(SHORT_QUEUE)
        .then(ok => {
            if (ok.messageCount < SHORT_QUEUE_DNO) {
                var sendCount = 2 * SHORT_QUEUE_DNO - ok.messageCount;
                var promise = Promise.resolve();

                for (let i = 0; i < sendCount; i++) {
                    promise = promise
                        .then(() => messagePool.next().then(messageHandler(ch)));
                }

                promise
                    .catch(err => {
                        error(err);
                    })
                    .then(() => {
                        timeout(10, ch).then(checkShortQueue);
                    });
            } else {
                timeout(10, ch).then(checkShortQueue);
            }
        })
}

function messageHandler(ch) {
    return msgHolder => {
        try {
            var command = msgHolder.msg;

            var actualTargetIds = _.filter(command.targetIds, targetId =>
                !cancellationStore.has(command.accountId, command.metricSetup.id, targetId, command.eventId)
            );

            if (actualTargetIds.length) {
                command = new CalculateMetricCommand({
                    accountId: command.accountId,
                    metricSetup: command.metricSetup,
                    targetIds: actualTargetIds,
                    eventId: command.eventId,
                    commandId: command.commandId
                });

                // TODO: Probably we need to use ConfirmChannel to get ack from the server\
                // TODO: that sent message is received
                ch.sendToQueue(SHORT_QUEUE, command.toBuffer());
                
                log(`Sent CalculateMetricCommand#${command.commandId}`,
                    `CommandId=${command.commandId}`,
                    `EventId=${command.eventId}`,
                    `AccountId=${command.accountId}`,
                    `MetricSetupId=${command.metricSetup.id}`,
                    `TargetCount=${command.targetIds.length}`);

                var processingFinishedEvent = new ProcessingFinishedEvent({
                    accountId: command.accountId,
                    metricSetupId: command.metricSetup.metricId,
                    targetIds: actualTargetIds,
                    eventId: command.eventId});

                ch.sendToQueue(PROCESSING_FINISHED_QUEUE, processingFinishedEvent.toBuffer());
                log(` Sent to ${PROCESSING_FINISHED_QUEUE} #${command.commandId}`);
            } else {
                log(`Cancelled CalculateMetricCommand`,
                    `CommandId=${command.commandId}`,
                    `EventId=${command.eventId}`);
            }

            msgHolder.ack();
        } catch (e) {
            msgHolder.nack();
            throw e;
        }
    };
}

function timeout(ms, ...args) {
    return new Promise(res => {
        setTimeout(() => {
            res(...args);
        }, ms);
    });
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
