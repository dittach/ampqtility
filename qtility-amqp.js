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
        handlePersist2();
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


function handlePersist2() {
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


if (program.op === "test") {
   // handleTestAsync();
} else if (1>2) {

    amqp(app, amqpSettings).connect(function () {
        var debugThisFunction = true;
        var fName = 'amqp.copnnect():';

        if (process.send) process.send('online');
        //do stuff

        createTempQueue(function () {
            if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'createTempQueue callback. op:', program.op); }
            if (program.op === "persist") {
                handlePersist();
            } else if (program.op === "movequeues") {
                //sourcequeue
                //destqueue
            }/* else if (program.op === "test") {
            handleTest();
        }*/

        });

        app.ready = true;
    });
}

async function handleTestAsync() {
    const debugThisFunction = true;
    const fName = 'handleTestAsync():';

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called. source:', program.sourcequeue); }

    const conn = await myamqp.connect('amqp://dittach_staging:4QGe6CEZyf9q4dlzj7E47ayW@amqp.local.staging.dittach.com:5672/dittach_staging');
    const channel = await conn.createChannel();

    var content = { "test1": "test1" };
    await channel.assertExchange(program.sourcequeue, exchangeOptions.type, { durable: true });
    await channel.publish(program.sourcequeue, exchangeBindings, new Buffer(JSON.stringify(content)) );
    content = { "test2": "test2" };
    await channel.publish(program.sourcequeue, exchangeBindings, new Buffer(JSON.stringify(content)) );
    content = { "test3": "test3" };
    await channel.publish(program.sourcequeue, exchangeBindings, new Buffer(JSON.stringify(content)) );
    await channel.close();
    await conn.close();
}


function handleTest() {
    var debugThisFunction = true;
    var fName = 'handleTest():';
    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called.'); }

    console.log('Adding 3 test messages to queue', program.sourcequeue);
    var messageOptions = {
        mandatory: true,
        contentType: 'application/json',
        deliveryMode: 2, // persistent
        persistent: true
    };
    var content = {"test1":"test1"};
    console.log('1:', app.amqp.publish(program.sourcequeue, exchangeBindings, new Buffer(JSON.stringify(content)), messageOptions));
    content = {"test2":"test2"};
    console.log('2:', app.amqp.publish(program.sourcequeue, exchangeBindings, new Buffer(JSON.stringify(content)), messageOptions));
    content = {"test3":"test3"};
    console.log('3:', app.amqp.publish(program.sourcequeue, exchangeBindings, new Buffer(JSON.stringify(content)), messageOptions));
    console.log('done');
    setTimeout(function() { cleanupAndShutdown(); }), 5000;
}

function handlePersist() {
    var debugThisFunction = true;
    var fName = 'handlePersist():';

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called.'); }

    app.amqpHelpers.subscribeDirect(program.sourcequeue, function (error, content, message, messageCount) {
        if (error) return app.amqp.reject(message);

        if (queuePoll === undefined) {
            queuePoll = setInterval(endWhenEmpty, 5000);
        }

        if (!content) {
            console.log("malformed message received on", program.sourcequeue, content, message);
            return app.amqp.reject(message);
        }

        console.log("messageCount:", messageCount);
        console.log("received new", program.sourcequeue);

        var messageOptions = {
            mandatory: true,
            contentType: 'application/json',
            deliveryMode: 2, // persistent
            persistent: true
        };

        if (message.properties !== null) {
            _.extend(messageOptions, message.properties);
        }
        _.extend(messageOptions, {
            deliveryMode: 2,
            persistent: true
        });

        if (messageCount > 0) {
            try {
                app.amqp.publish(tempqueue, exchangeBindings, new Buffer(JSON.stringify(content)), messageOptions);
                app.amqp.ack(message, false);
            } catch (e) {
                console.log("qtility", "Unable to publish on", tempqueue, exchangeBindings, "with message", JSON.stringify(message), "error:", e.message);
                app.amqp.reject(message);
            }
        } else {
            console.log('No messages found. Exiting.');
            cleanupAndShutdown();
        }
    });
}

function createTempQueue(callback) {
    var debugThisFunction = true;
    var fName = 'createTempQueue():';

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called.'); }
    app.amqp.assertExchange(tempqueue, exchangeOptions.type).then(function (ex) {
        app.amqp.assertQueue(tempqueue, {
            durable: true,
            autoDelete: false,
            confirm: true
        }).then(function (q) {
            app.amqp.bindQueue(tempqueue, tempqueue, exchangeBindings).then(function () {
                console.log('qtility', 'queue', tempqueue, 'bound successfully on exchange', tempqueue, exchangeBindings);
            }).catch(function (err) {
                console.log('qtility', 'error during queue setup', err);
                callback(err);
            });
        });
    });

    callback(null);
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

function endWhenEmpty() {
    var debugThisFunction = true;
    var fName = 'endWhenEmpty():';

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called.'); }
    getQueueCount(program.sourcequeue, function (messageCount) {
        console.log('endWhenEmpty(), messageCount:', messageCount);

        if (messageCount === 0) {
            if (queuePoll !== undefined) {
                clearInterval(queuePoll);
            }

            // unsubscribe from the source queue
            unsubscribeFromQueueAndExchange(program.sourcequeue, moveItemsBack);
            //cleanupAndShutdown();
        }
    });
}

function unsubscribeFromQueueAndExchange(queueName, callback) {
    var debugThisFunction = true;
    var fName = 'unsubscribeFromQueueAndExchange():';

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called.'); }
    app.amqp.cancel('qtility').then(function () {
        callback();
    });
}

function moveItemsBack(err) {
    var debugThisFunction = true;
    var fName = 'moveItemsBack():';

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called.'); }
    // subscribe to tempqueue as a consumer
    if (err) {
        console.log('Unable to unsubscribe from source queue', program.sourcequeue);
        cleanupAndShutdown();
    } else {
        subscribeToTempQueue(function () {
            if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'subscribeToTempQueue() callback.'); }
        });
    }

}


function subscribeToTempQueue(callback) {
    var debugThisFunction = true;
    var fName = 'subscribeToTempQueue():';

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called.'); }


    app.amqpHelpers.subscribeDirect(tempqueue, function (error, content, message, messageCount) {

        if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'subscribeDirect() callback.'); }

        if (error) return app.amqp.reject(message);

        if (queueEndPoll === undefined) {
            if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'subscribeDirect() callback: initializing queueEndPoll setInterval'); }
            queueEndPoll = setInterval(endMoveBackWhenEmpty, 5000);
        }

        if (!content) {
            console.log("malformed message received on", tempqueue, content, message);
            return app.amqp.reject(message);
        }

        console.log("messageCount:", messageCount);
        console.log("received new", tempqueue);

        if (messageCount > 0) {
            if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'subscribeDirect(): messageCount', messageCount); }
            try {
                app.amqp.publish(program.sourcequeue, exchangeBindings, new Buffer(JSON.stringify(content)), message.properties);
                app.amqp.ack(message, false);
                if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'subscribeDirect(): published to', program.sourcequeue,'and ackd.'); }
            } catch (e) {
                console.log("qtility", "Unable to publish on", program.sourcequeue, exchangeBindings, "with message", JSON.stringify(message), "error:", e.message);
                app.amqp.reject(message);
            }
        }

    });

    callback(null);
}


function endMoveBackWhenEmpty() {
    var debugThisFunction = true;
    var fName = 'endMoveBackWhenEmpty():';

    if (enableDebugMsgs && debugThisFunction) { console.log(modName, fName, 'called.'); }
    getQueueCount(tempqueue, function (messageCount) {
        console.log('endMoveBackWhenEmpty(), messageCount:', messageCount);

        if (messageCount === 0) {
            if (queueEndPoll !== undefined) {
                clearInterval(queueEndPoll);
            }
            cleanupAndShutdown();
        }
    });
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
    
    // delete temp queue and exchange
    app.amqp.deleteQueue(tempqueue,{'ifEmpty':true});
    app.amqp.deleteExchange(tempqueue);

    // cleanly disconnect from AMQP
    app.amqp.close();

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