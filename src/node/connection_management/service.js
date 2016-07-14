const amqp = require('amqplib');

const QUEUE1 = 'queue1';
const QUEUE2 = 'queue2';

var messageIncrement = 1;
const RABBIT_SERVER_URL = 'amqp://192.168.99.100';

class Service {
    constructor(ch) {
        this._channel = ch;
    }

    run() {
        return Promise.all([
            this._channel.assertQueue(QUEUE1, {durable: false}),
            this._channel.assertQueue(QUEUE2, {durable: false})
        ]).then(([q1, q2]) => {
            this._channel.consume(QUEUE1, this._consumeMessage.bind(this), {noAck: true});
            this._generateMessages();
            console.log('service started');
        }).catch((err) => {
            console.log('service start failed');
            console.error(err);
        });

    }

    _consumeMessage(msg) {
        this._channel.sendToQueue(QUEUE2, new Buffer(msg.content.toString()));
    }

    _generateMessages() {
        this._generator = setInterval(() => {
            this._channel.sendToQueue(QUEUE1, new Buffer(`message ${messageIncrement++}`));

        }, 1000);
    }

    destroy() {
        clearInterval(this._generator);
        this._generator = null;
        try {
            this._channel.close();
        } catch (e) {
            console.error(e);
        }
    }
}



const applicationConfig = {
    serviceGroups: [
        {
            channelProvider: conn => conn.createChannel(),
            services: [Service]
        }
    ]

};


class Application {
    constructor() {
        this._toDoOnReset = [];
    }

    run() {
        this._run()
            .then(() => {
                console.log('app started');
            });
    }

    _run() {
        return this._createConnection()
            .then(conn => {
                var promises = [];
                applicationConfig.serviceGroups.forEach(group => {
                    promises.push(group.channelProvider(conn)
                        .then(ch => {
                            ch.on('error', (err) => {
                                console.log('channel error');
                                console.error(err);
                            });

                            return Promise.all(group.services.map(type => {
                                var service = new type(ch);

                                this._toDoOnReset.push(() => {
                                    service.destroy();
                                });

                                return service.run();
                            }));
                        }));
                });

                return Promise.all(promises);

            });
    }

    _createConnection() {
        return amqp.connect(RABBIT_SERVER_URL, {heartbeat: 10000})
            .then(conn => {
                conn.on('error', this._handleError.bind(this));
                console.log('connection created');
                return conn;
            })
            .catch(err => {
                console.log('error on creation connection');
                console.error(err);
                return new Promise(res => {
                    setTimeout(() => {
                        res(this._createConnection());
                    }, 1000);
                })
            });
    }

    _handleError(err) {
        console.log('error while app lifecycle');
        console.error(err);
        var forReset = this._toDoOnReset;
        this._toDoOnReset = [];

        forReset.forEach(handler => {
            handler();
        });

        setTimeout(() => {
            this._run();
        }, 1000);

    }
}




var app = new Application();

app.run();


