const _ = require('underscore');
const fs = require('fs');
const amqp = require('amqplib');
const timeSpan = require('timespan');
const ProtoBuf = require('protobufjs');

const builder = ProtoBuf.loadProtoFile('./scheduled_producer.proto');
const MetricSetupChangedEvent = builder.build('MetricSetupChangedEvent');

const SOURCE_QUEUE = 'metric_setup_events';
const RABBIT_SERVER_URL = 'amqp://192.168.99.100';

const fileName = process.argv[2]; // || './data/default.json';

if (!fileName) {
    console.error(' Missed fileName');
    return;
}

prepareChannel()
    .then(processFile(fileName))
    .then(() => {
        process.exit();
    })
    .catch(err => {
        console.error(' ERROR', err);
    });

function prepareChannel() {
    return amqp.connect(RABBIT_SERVER_URL)
        .then(conn => conn.createChannel())
        .then(ch => ch.assertQueue(SOURCE_QUEUE, {durable: false}).then(() => ch));
}

function processFile(fileName) {
    return ch =>
        readFile(fileName)
            .then(fileStr =>
                Promise.all(
                    _.map(JSON.parse(fileStr), item => {
                        var ts = parseTimeSpan(item.fireTimeSpan);

                        return timeout(ts.totalMilliseconds())
                            .then(() => {
                                var message = new MetricSetupChangedEvent({
                                    metricSetup: _.clone(item.changeEvent.metricSetup),
                                    accountId: item.changeEvent.accountId,
                                    entityTypes: item.changeEvent.entityTypes
                                });

                                ch.sendToQueue(SOURCE_QUEUE, message.toBuffer());

                                console.log(` [${item.fireTimeSpan}]`, JSON.stringify(message));
                            });
                    })))
            .then(() => {
                console.log(' All messages were sent');
            })
}

function readFile(fileName) {
    return new Promise((res, rej) => {
        fs.readFile(fileName, 'utf8', (err, data) => {
            if (err) {
                rej(err);
                return;
            }

            res(data);
        });
    });
}

function parseTimeSpan(str) {
    var separated = _.flatten(
        str.split('.').map(x => x.split(':'))
    ).map(item => parseInt(item));

    return new timeSpan.TimeSpan(separated[3], separated[2], separated[1], separated[0]);
}

function timeout(ms) {
    return new Promise(res => {
        setTimeout(res, ms);
    });
}
