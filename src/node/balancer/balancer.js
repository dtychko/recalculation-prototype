const _ = require('underscore');
const ProtoBuf = require('protobufjs');
const amqp = require('amqplib');
const Guid = require('guid');
const queueRegistry = require('./balancer.queue_registry');
const messagePool = require('./balancer.message_pool');

const builder = ProtoBuf.loadProtoFile('./balancer.proto');
const RequestQueuesCommand = builder.build('RequestQueuesCommand');
const QueuesCollection = builder.build('QueuesCollection');
const QueueChangedEvent = builder.build('QueueChangedEvent');
const CalculateMetricCommand = builder.build('CalculateMetricCommand');

const RABBIT_SERVER_URL = 'amqp://192.168.99.100';
const QUEUE_DISCOVERY_QUEUE = 'queue_discovery_queue';
const QUEUE_CHANGED_EXCHANGE = 'queue_changed_queue';
const CANCELLATION_QUEUE = 'cancellation_queue';
const SHORT_QUEUE = 'short_queue';
const CHANNEL_PREFETCH = 100;
const SHORT_QUEUE_DNO = 50;

createChannel()
    .then(subscribeToQueueChangedExchange)
    .then(discoverQueues)
    .then(initConsumers)
    .then(initShortQueue)
    .then(() => {
        console.log(' [x] App Started');
    })
    .catch(err => {
        console.error(err);
    });

function createChannel() {
    return amqp.connect(RABBIT_SERVER_URL)
        .then(conn => conn.createChannel())
        .then(ch => ch.prefetch(CHANNEL_PREFETCH).then(() => ch))
        .then(ch => {
            console.log(' [1] Connected and created channel.');
            return ch;
        });
}

function subscribeToQueueChangedExchange(ch) {
    return Promise.all([
            ch.assertExchange(QUEUE_CHANGED_EXCHANGE, 'fanout', {durable: false}),
            ch.assertQueue('', {exclusive: true, durable: false})
        ])
        .then(([ex, q]) => ch.bindQueue(q.queue, ex.exchange, '').then(() => q))
        .then(q => ch.consume(q.queue, msg => {
            console.log(` [x] Received QueueChangedEvent.`);

            const event = QueueChangedEvent.decode(msg.content);

            if (event.modification === 1) {
                queueRegistry.add(event.queueName);
            } else if (event.modification === 2) {
                queueRegistry.remove(event.queueName);
            }
        }, {noAck: true}))
        .then(() => {
            console.log(` [3] Subscribed to ${QUEUE_CHANGED_EXCHANGE} exchange.`);
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

                ch.consume(responseQ.queue, msg => {
                        if (msg.properties.correlationId === correlationId) {
                            var collection = QueuesCollection.decode(msg.content);

                            _.each(collection.queueNames, q => {
                                queueRegistry.add(q);
                            });

                            ch.cancel(consumerTag);

                            console.log(` [4] Received existing queue names: [${_.toArray(collection.queueNames).join(', ')}]`);
                            res(ch);
                        }
                    }, {noAck: true})
                    .then(ok => {
                        consumerTag = ok.consumerTag;
                        ch.sendToQueue(requestQ.queue, new RequestQueuesCommand({}).toBuffer(), {
                            correlationId: correlationId,
                            replyTo: responseQ.queue
                        });

                        console.log(` [4] Sent request for queue discovery.`);
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
        return ch.consume(queueName, msg => {
            var command = CalculateMetricCommand.decode(msg.content);
            messagePool.add(command, () => {
                ch.ack(msg);
            });
        }).then(ok => {
            queueNameToConsumer[queueName].consumerTag = ok.consumerTag;
        });
    }

    queueRegistry.on('add', initConsumer);
    // TODO: implement queueRegistry.on('remove', queueName => { ... });

    return Promise.all(_.map(queueRegistry.getAll(), initConsumer))
        .then(() => {
            console.log(' [5] Initialized consumers.');
            return ch;
        });
}

function initShortQueue(ch) {
    return ch.assertQueue(SHORT_QUEUE, {durable: false})
        .then(q => {
            setInterval(() => {
                ch.checkQueue(SHORT_QUEUE)
                    .then(ok => {
                        if (ok.messageCount < SHORT_QUEUE_DNO) {
                            messagePool.next(2 * SHORT_QUEUE_DNO - ok.messageCount, msgs => {
                                _.each(msgs, msg => {
                                    ch.sendToQueue(SHORT_QUEUE, msg.toBuffer() /*new CalculateMetricCommand(msg).toBuffer()*/);
                                })
                            })
                        }
                    })
            }, 10);
        })
        .then(() => {
            console.log(' [6] Initialized short queue.');
            return ch;
        });
}
