const amqp = require('amqplib');
const readline = require('readline');
const fileWrapper = require('./file_wrapper');
const timeSpan = require('timespan');
const ProtoBuf = require('protobufjs');
const _ = require('underscore');

const builder = ProtoBuf.loadProtoFile('./scheduled_producer.proto');
const MetricSetupChangedEvent = builder.build('MetricSetupChangedEvent');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

const SOURCE_QUEUE = 'metric_setup_events';
const RABBIT_SERVER_URL = 'amqp://192.168.99.100';

const executeOnQuit = [];

createChannel()
    .then(initQueue)
    .then(initFileProcessor)
    .then(initConsoleInput);


function processFile({fileName, ch}) {
    return fileWrapper.readFile(fileName || './data/default.json')
        .then(fileStr => {
            var json = JSON.parse(fileStr);
            var promises = [];

            for (var i = 0; i < json.length; i++) {
                let jsonItem = json[i];
                console.log(jsonItem);
                var separated = jsonItem.fireTimeSpan
                    .split(':')
                    .map(item => parseInt(item));
                let ts = new timeSpan.TimeSpan(separated[3],
                    separated[2],
                    separated[1],
                    separated[0]);

                promises.push(new Promise(res => {
                    setTimeout(() => {
                        var message = new MetricSetupChangedEvent({
                            metricSetup: _.clone(jsonItem.changeEvent.metricSetup),
                            accountId: jsonItem.changeEvent.accountId,
                            entityTypes: jsonItem.changeEvent.entityTypes
                        });

                        ch.sendToQueue(SOURCE_QUEUE, message.toBuffer());
                        console.log(` [x] message sent to queue.`);
                        res();
                    }, ts.totalMilliseconds());
                }));
            }
            return Promise.all(promises);
        });
}

function processWrongCommand() {
    console.log('Wrong Command!!!')
}

function processCommand({command, parameters, fs}) {
    return new Promise((res) => {
        switch (command) {
            case 'r':
            case 'read':
                fs(parameters)
                    .then(() => {
                        res();
                    }, () => {
                        res();
                    });
                return;
            case 'q':
            case 'quit':
                executeOnQuit.forEach(e => {
                    e();
                });
                process.exit(0);
                break;
            default:
                processWrongCommand();
        }
        res();
    });
}

function parseCommand(commandRaw, fs) {
    var command;
    var parameters;
    try {
        var splitted = commandRaw.split(' ');
        command = splitted[0];

        splitted = splitted.slice(1, splitted.length);

        parameters = _.toArray(_.groupBy(splitted, (el, index) => Math.floor(index / 2)))
            .reduce((memo, item) => {
                memo[item[0]] = item[1].replace('"', '');
                return memo;
            }, {});
    } catch (e) {
        console.error(e);
        return new Promise(res => {
            res();
        });
    }

    return new Promise(res => {
        res({command, parameters, fs})
    });
}

function createChannel() {
    return amqp.connect(RABBIT_SERVER_URL)
        .then(conn => {
            return conn.createChannel()
                .then(ch => {
                    executeOnQuit.push(() => {
                        ch.close();
                        conn.close();
                        console.log(" [x] all connections closed.")
                    });
                    return ch;
                });
        });
}

function initQueue(ch) {
    ch.assertQueue(SOURCE_QUEUE, {durable: false});
    return ch;
}

function initFileProcessor(ch) {
    return ({fileName}) => processFile({fileName, ch});
}

function initConsoleInput(fs) {
    function recursiveReadLine() {
        console.log('');
        rl.question('Enter command: ', (commandRaw) => {
            console.log('You\'ve entered:', commandRaw);

            parseCommand(commandRaw, fs)
                .then(processCommand)
                .then(() => {
                    recursiveReadLine();
                });
        });
    }

    recursiveReadLine();
}








