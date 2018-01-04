'use strict';

const program = require('commander');
const request = require('request');
const http = require('http');
const fs = require('fs');
const _ = require('lodash');
const async = require('async');
const myamqp = require('amqplib');

var queuePoll;
var queueEndPoll;
var app = Object;

const enableDebugMsgs = true;
const modName = 'qtility-amqp.js:';

program
    .version('0.0.1')
    .option('-h, --host <host>', 'AMQP Host')
    .option('-p, --port [port]', 'Port (5672)', parseInt, 5672)
    .option('-v, --vhost <vhost>', 'Vhost')
    .option('-l, --login <login_name>', 'Login Name')
    .option('-x, --password <password>', 'Password')
    .option('-o, --op <op>', 'Operation', /^(persist|movequeues|test)$/i, 'persist')
    .option('-s, --sourcequeue <sourcequeue>', 'Source Queue Name')
    .option('-d, --destqueue <destqueue>', 'Dest Queue Name')
    .option('-t, --testamt <n>', 'Test Amount (3)', parseInt)
    .parse(process.argv);

const amqp = require('./lib/amqp');
const amqpSettings = require('./config/amqp_config.js');

_.extend(amqpSettings, {
    host: program.host,
    port: program.port,
    vhost: program.vhost,
    login: program.login,
    password: program.password
});

let missing_options = [];
if (amqpSettings.host.length === 0) missing_options.push("host");
if (amqpSettings.port === 0) missing_options.push("port");
if (amqpSettings.vhost.length === 0) missing_options.push("vhost");
if (amqpSettings.login.length === 0) missing_options.push("login");
if (amqpSettings.password.length === 0) missing_options.push("password");
if (program.op.length === 0) missing_options.push("op");
if (program.sourcequeue.length === 0) { missing_options.push("sourcequeue"); }

if (program.op === "movequeues" && program.destqueue.length === 0) {
    missing_options.push("destqueue");
}
if (program.op === "test" && (program.testamt > 1000 || program.testamt < 1)) {
    missing_options.push("testamt");
}

if (missing_options.length > 0) {
    console.error("required options:", missing_options.join(", "));
    process.exit();
}

const timestamp = Date.now().toString();
const tempqueue = 'qtility.temp.' + timestamp;
var exchangeBindings = '#';
const exchangeOptions = {
    type: 'topic',
    durable: true,
    autoDelete: false,
    confirm: true
};

require('./lib/amqp')(app, amqpSettings);

if (enableDebugMsgs) { console.log(modName, 'program.op:', program.op); }
switch (program.op) {
    case 'test':
        handleTestAsync();
    break;
    case 'persist':
        handlePersist();
    break;
    case 'movequeues':
        handleMoveQueues();
    break;
    default:
        program.outputHelp();
    break;
}

function handleMoveQueues() {
    const debugThisFunction = true;
    const fName = 'handleMoveQueues():';

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called.'); }

    // connect to amqp
    amqp(app, amqpSettings).connect(async function() {
        await moveQueues(program.sourcequeue, program.destqueue);
        
        cleanupAndShutdown()
    });
    
}

async function moveQueues(srcqueue, destqueue, options) {
    const debugThisFunction = true;
    const fName = 'moveQueues():';
    const consumerTag = 'qtility-' + Date.now().toString();

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called.'); }
    try {
        // check that both queues exist
        await app.amqp.checkQueue(srcqueue);
        await app.amqp.checkQueue(destqueue);

        // consume from srcqueue
        await app.amqp.consume(srcqueue, function (message) {
            let content;
            let messageOptions= {};

            //  start a timer to listen for empty
            if (typeof queuePoll === 'undefined') {
                queuePoll = setInterval(function() { cancelWhenEmpty(srcqueue,consumerTag); }, 5000);
            }

            try {
              content = JSON.parse(message.content.toString());
            } catch (error) {
              console.error("error parsing AMQP message", message, message.content, "on queue", srcqueue, error);
              return error;
            }

            //  onMessage: publish to destqueue, use options if they exist
            if (message.properties !== null) {
                _.extend(messageOptions, message.properties);
            }
            if (options!==undefined && options!==null) {
                _.extend(messageOptions, options);
            }
            /*
            _.extend(messageOptions, {
                deliveryMode: 2,
                persistent: true
            });*/
            
            try {
                app.amqp.publish(destqueue, exchangeBindings, new Buffer(JSON.stringify(content)), messageOptions);
                app.amqp.ack(message, false);
            } catch (e) {
                console.log("qtility", "Unable to publish on", destqueue, exchangeBindings, "with message", JSON.stringify(content), "error:", e.message);
                app.amqp.reject(message);
            }
          }, {
            noAck: false,
            consumerTag: consumerTag
          });
    
    } catch (error) {
        console.log(error.message);
    }
}



function cancelWhenEmpty(queueName, consumerTag) {
    const debugThisFunction = true;
    const fName = 'cancelWhenEmpty():';

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called.'); }
    getQueueCount(queueName, function (messageCount) {
        if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, ', messageCount:', messageCount); }

        if (messageCount === 0) {
            if (typeof queuePoll !== 'undefined') {
                clearInterval(queuePoll);
                queuePoll=undefined;
            }
            app.amqp.cancel(consumerTag);
        }
    });
}


function handlePersist() {
    // connect to amqp
    amqp(app, amqpSettings).connect(async function() {
        try {
            //create tempqueue
            await app.amqp.assertExchange(tempqueue, exchangeOptions.type);
            await  app.amqp.assertQueue(tempqueue, {
                durable: true,
                autoDelete: false,
                confirm: true
            });
            await app.amqp.bindQueue(tempqueue, tempqueue, exchangeBindings);
            
            //move items from source to tempqueue, marking as persistent
            await moveQueues(program.sourcequeue, tempqueue, {
                deliveryMode: 2,
                persistent: true
            });

            //move things from tempqueue back to source
            await moveQueues(tempqueue, program.sourcequeue, {
                deliveryMode: 2,
                persistent: true
            });
        } catch (error) {
            console.log(error.message);
        }
        
        // clean up
        cleanupAndShutdown()
    });
}

async function handleTestAsync() {
    const debugThisFunction = true;
    const fName = 'handleTestAsync():';

    const numTests = (typeof program.testamt !== 'undefined' && program.testamt !== null && program.testamt>0)?program.testamt:3;

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called. source:', program.sourcequeue, ', program.testamt:', program.testamt, ', numTests:', numTests); }

    const conn = await myamqp.connect('amqp://dittach_staging:4QGe6CEZyf9q4dlzj7E47ayW@amqp.local.staging.dittach.com:5672/dittach_staging');
    const channel = await conn.createChannel();
    let content = {};

    await channel.assertExchange(program.sourcequeue, exchangeOptions.type, { durable: true });

    try {
        for (let i=0; i < numTests; i++) {
            content = { "test": "test"+i };
            await channel.publish(program.sourcequeue, exchangeBindings, new Buffer(JSON.stringify(content)) );
        }
    } catch (error) {
        console.log('an error:', error.message);
    }
/*
    content = { "test2": "test2" };
    await channel.publish(program.sourcequeue, exchangeBindings, new Buffer(JSON.stringify(content)) );
    content = { "test3": "test3" };
    await channel.publish(program.sourcequeue, exchangeBindings, new Buffer(JSON.stringify(content)) );*/
    await channel.close();
    await conn.close();
}

function getQueueCount(queueName, emptyCallback) {
    var options = options || {};
    options = _.omit(options, "type");

    async.waterfall([
        buildDefaults,
        initQueue,
        isEmpty
    ]);

    function buildDefaults(callback) {
        callback(null, _.extend({
            durable: true,
            autoDelete: false
        }, options));
    }

    function initQueue(queueOptions, callback) {
        callback(null, app.amqp.assertQueue(queueName, queueOptions));
    }

    function isEmpty(ok, callback) {
        ok.then(function (results) {
            console.log('results:', results);
            emptyCallback(results.messageCount);
        });
    }
}


process.on('SIGTERM', cleanupAndShutdown);
process.on('SIGINT', cleanupAndShutdown);

process.on('message', function (message) {
    if (message === 'shutdown') cleanupAndShutdown();
});

function cleanupAndShutdown() {
    if (typeof queuePoll !== 'undefined') {
        clearInterval(queuePoll);
    }
    
    try {
        // delete temp queue and exchange
        app.amqp.deleteQueue(tempqueue,{'ifEmpty':true});
        app.amqp.deleteExchange(tempqueue);

        // cleanly disconnect from AMQP
        app.amqp.close();
    } catch (error) {
        console.log('An error occurred:', error.message);
    }
    _.values(app.listeners).forEach(function (listener) {
        listener && listener.stop();
    });

    // give some time for things to settle (IMAP disconnect may not make
    // it in time if it has a lot of things queued up)
    setTimeout(function () {
        process.exit(0);
    }, 5000);
}

module.exports = app;