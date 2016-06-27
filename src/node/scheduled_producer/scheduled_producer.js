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
    .then(initConsoleInput);

function createChannel() {
    return amqp.connect(RABBIT_SERVER_URL)
        .then(conn => conn.createChannel()
            .then(ch => {
                executeOnQuit.push(() => {
                    ch.close();
                    conn.close();
                    console.log(" [x] all connections closed.")
                });
                return ch;
            })
        );
}

function initQueue(ch) {
    return ch.assertQueue(SOURCE_QUEUE, {durable: false})
        .then(() => ch);
}

function processFile({fileName, ch}) {
    return fileWrapper.readFile(fileName || './data/default.json')
        .then(fileStr => {
            var json = JSON.parse(fileStr);
            var promises = [];

            for (var i = 0; i < json.length; i++) {
                let jsonItem = json[i];
                console.log(jsonItem);
                var separated = _.flatten(
                    jsonItem.fireTimeSpan
                        .split('.')
                        .map(x => x.split(':'))
                ).map(item => parseInt(item));

                console.log(separated);

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


function initConsoleInput(ch) {
    function recursiveReadLine() {
        console.log('');
        rl.question('Enter command: ', (commandRaw) => {
            console.log('You\'ve entered:', commandRaw);

            parseCommand(commandRaw)
                .then(x => processCommand(x, ch))
                .then(recursiveReadLine);
        });
    }

    recursiveReadLine();
}

function processWrongCommand() {
    console.log('Wrong Command!!!')
}

function processCommand({command, parameters}, ch) {
    return new Promise((res) => {
        switch (command) {
            case 'r':
            case 'read':
                processFile({
                    fileName: parameters.fileName,
                    ch: ch
                }).then(() => {
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

function parseCommand(commandRaw) {
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

    return Promise.resolve({
        command,
        parameters
    });
}
