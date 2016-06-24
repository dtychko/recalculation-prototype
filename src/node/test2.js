// var amqp = require('amqplib/callback_api');
var amqp = require('amqplib');

amqp.connect('amqp://192.168.99.100')
    .then(conn => {
        conn.createChannel()
            .then(ch => {
                ch.consume('metric_setup_events', msg => {

                }).then(ok => {
                    console.log(ok);
                }, err => {
                    console.error(err);
                });

                //console.log(result);
            });
    });

