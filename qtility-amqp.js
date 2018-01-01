'use strict';

const program = require('commander');
const request = require('request');
const http = require('http');
const fs = require('fs');
const _ = require('lodash');
const async = require('async');

var app = Object;

program
  .version('0.0.1')
  .option('-h, --host <host>', 'AMQP Host')
  .option('-p, --port [port]', 'Port (5672)', parseInt, 5672)
  .option('-v, --vhost <vhost>', 'Vhost')
  .option('-l, --login <login_name>', 'Login Name')
  .option('-x, --password <password>', 'Password')
  .parse(process.argv);

var amqp = require('./lib/amqp');
var amqpSettings = require('./config/amqp_config.js');

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

if (missing_options.length > 0) {
    console.error("required options:", missing_options.join(", "));
    process.exit();
}

require('./lib/amqp')(app, amqpSettings);

amqp(app, amqpSettings).connect(function(){
    if (process.send) process.send('online');
//do stuff

    app.ready = true;
});

process.on('SIGTERM', cleanupAndShutdown);
process.on('SIGINT',  cleanupAndShutdown);

process.on('message', function(message) {
  if (message === 'shutdown') cleanupAndShutdown();
});

function cleanupAndShutdown(){
  // cleanly disconnect from AMQP
  app.amqp.close();

  _.values(app.listeners).forEach(function(listener){
    listener && listener.stop();
  });

  // give some time for things to settle (IMAP disconnect may not make
  // it in time if it has a lot of things queued up)
  setTimeout(function(){
    process.exit(0);
  }, 5000);
}

module.exports = app;
