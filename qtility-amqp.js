'use strict';

const program = require('commander');
const request = require('request');
const http = require('http');
const fs = require('fs');
const _ = require('lodash');
const async = require('async');

var queuePoll;
var app = Object;

program
  .version('0.0.1')
  .option('-h, --host <host>', 'AMQP Host')
  .option('-p, --port [port]', 'Port (5672)', parseInt, 5672)
  .option('-v, --vhost <vhost>', 'Vhost')
  .option('-l, --login <login_name>', 'Login Name')
  .option('-x, --password <password>', 'Password')
  .option('-o, --op <op>', 'Operation', /^(persist|movequeues)$/i, 'persist')
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
if (program.sourcequeue.length === 0) {
  missing_options.push("sourcequeue");
}

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

amqp(app, amqpSettings).connect(function () {
  if (process.send) process.send('online');
  //do stuff

  createTempQueue(function () {
    if (program.op === "persist") {
      handlePersist();
    } else if (program.op === "movequeues") {
      //sourcequeue
      //destqueue
    }

  });

  app.ready = true;
});

function handlePersist() {
  app.amqpHelpers.subscribeDirect(program.sourcequeue, function (error, content, message, messageCount) {
    if (error) return app.amqp.reject(message);

    if (queuePoll===undefined) {
      queuePoll = setInterval(endWhenEmpty, 5000);
    }

    if (!content) {
      console.log("malformed message received on", program.sourcequeue, content, message);
      return app.amqp.reject(message);
    }

    console.log("messageCount:",messageCount);
    console.log("received new", program.sourcequeue);

    var messageOptions = {
      mandatory: true,
      contentType: 'application/json',
      deliveryMode: 2 // persistent
    };

    if (message.properties !== null) {
      _.extend(messageOptions, message.properties);
    }
    _.extend(messageOptions, {deliveryMode: 2});

    if (messageCount>0) {

      try {
        app.amqp.publish(tempqueue, exchangeBindings, new Buffer(JSON.stringify(content)), messageOptions);
        app.amqp.ack(message, false);
      } catch (e) {
        console.log("qtility", "Unable to publish on", tempqueue, exchangeBindings, "with message", JSON.stringify(message), "error:", e.message);
        app.amqp.reject(message);
      }

    }
  });
}

function createTempQueue(callback) {
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
    ok.then(function(results) {
      console.log('results:', results);
      emptyCallback(results.messageCount);
    });
  }
}

function endWhenEmpty() {
  getQueueCount(program.sourcequeue, function(messageCount){
    console.log('endWhenEmpty(), messageCount:', messageCount);

    if (messageCount===0) {
      moveItemsBack();
      //cleanupAndShutdown();
    }
  });
}

function moveItemsBack() {

}



process.on('SIGTERM', cleanupAndShutdown);
process.on('SIGINT', cleanupAndShutdown);

process.on('message', function (message) {
  if (message === 'shutdown') cleanupAndShutdown();
});

function cleanupAndShutdown() {
  clearInterval(queuePoll);
// delete temp queue and exchange
  //app.amqp.deleteQueue(tempqueue,{'ifEmpty':true});
  //app.amqp.deleteExchange(tempqueue);

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