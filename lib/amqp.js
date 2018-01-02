'use strict';

var _ = require('lodash');
var async = require('async');
var amqp = require('amqplib');
var when = require('when');

/**
 * A shortcut to the common-case of broadcasting 'fanout' to an exchange
 * @param {Object} amqp settings
 * @param {Object} amqp exchange options https://github.com/postwait/node-amqp#connectionexchange
 * @returns {Object} message
 * @module amqp
 */
module.exports = function (app, amqpSettings, options) {
  options = options || {};

  var exchangeType;
  exchangeType = options.type || "fanout";
  options = _.omit(options, "type");

  var lib = {
    /**
     * A shortcut to the common-case of broadcasting 'fanout' to an exchange
     * @param {String} Name of the exchange
     * @param {Object} The message object with an optional meta key
     * @param {String} (optional) routing key
     * @params {Function} Callback
     * @returns {Object} message
     */
    broadcast: function (exchangeName, message, routingKey, apiCallback) {
      if (_.isFunction(arguments[2])) {
        apiCallback = arguments[2];
        routingKey = null;
      }

      // fanout ignores this
      routingKey = routingKey || exchangeName;
      message = composeMessage(message);

      async.waterfall([
        lib.ensureConnection,
        buildDefaults,
        connectToExchange,
        publish,
      ], apiCallback || function () {});

      function buildDefaults(connection, callback) {
        callback(null, options);
      }

      function connectToExchange(exchangeOptions, callback) {
        var ok = app.amqp.assertExchange(exchangeName, exchangeType, exchangeOptions);

        ok.then(function () {
          callback(null, exchangeName);
        }, function (error) {
          console.log("error connecting to AMQP exchange", exchangeName, error);
          callback(error);
        });
      }

      function publish(exchangeName, callback) {
        app.amqp.publish(exchangeName, routingKey, message, {
          persistent: true,
          contentType: "application/json"
        });

        // wait for the channel to flush before we call back
        // NOTE: this is not terribly efficient but it is the most
        // paranoid approach
        app.amqp.waitForConfirms().then(function () {
          callback(null, message);
        }, function (error) {
          callback(error);
        });
      }

      return message;
    },

    /**
     * A shortcut to the common-case of sending directly to a queue
     * @param {String} Name of the queue
     * @param {Object} The message object with an optional meta key
     * @params {Function} Callback
     * @returns {Object} message
     */
    direct: function (queueName, message, apiCallback) {
      message = composeMessage(message);

      async.waterfall([
        lib.ensureConnection,
        publish
      ], apiCallback || function () {});

      function publish(queueOptions, callback) {
        app.amqp.assertQueue(queueName, options);
        app.amqp.sendToQueue(queueName, message, {
          persistent: true,
          contentType: "application/json"
        });

        // wait for the channel to flush before we call back
        // NOTE: this is not terribly efficient but it is the most
        // paranoid approach
        app.amqp.waitForConfirms().then(function () {
          callback(null, message);
        }, function (error) {
          callback(error);
        });
      }

      return message;
    },

    subscribeThroughExchange: function (queueName, exchangeName, routingKey, onMessage) {
      async.waterfall([
        lib.ensureConnection,
        buildDefaults,
        initExchange,
        initQueue,
        subscribe
      ]);

      var subscribeOptions;

      function buildDefaults(connection, callback) {
        exchangeType = exchangeType || "fanout";

        callback(null, _.extend({
          durable: true,
          autoDelete: false,
          exclusive: false
        }, options))
      }

      function initExchange(exchangeOptions, callback) {
        subscribeOptions = exchangeOptions;

        app.amqp.assertExchange(exchangeName, exchangeType, subscribeOptions).
        then(function (exchange) {
          callback(null, exchange);
        }, function (error) {
          console.error("error connecting to", exchangeName, "exchange", error);
        });
      }

      function initQueue(okExchange, callback) {
        console.log("mail crawler is connected to", exchangeName);

        var ok = app.amqp.assertQueue(queueName, subscribeOptions);

        ok.then(function () {
          callback(null, ok);
        }, function (error) {
          console.error("error connecting to", queueName, "queue", error);
        })
      }

      function subscribe(okQueue, callback) {
        app.amqp.bindQueue(queueName, exchangeName, routingKey || "");

        app.amqp.consume(queueName, function (message) {
          var content;

          try {
            content = JSON.parse(message.content.toString());
          } catch (error) {
            console.error("error parsing AMQP message", message, message.content, "on queue", queueName, error);
            return onMessage(error);
          }

          onMessage(null, content, message);
        }, {
          noAck: false
        });
      }
    },

    /**
     * A shortcut to the common case of subscribing directly to a
     * queue without a custom exchange, with manual acking
     * @param {String} Name of the queue
     * @params {Function} onMessage receives error, content, message
     * @returns {Object} message
     */
    subscribeDirect: function (queueName, onMessage) {
      async.waterfall([
        lib.ensureConnection,
        buildDefaults,
        initQueue,
        subscribe
      ]);

      function buildDefaults(connection, callback) {
        callback(null, _.extend({
          durable: true,
          autoDelete: false
        }, options));
      }

      function initQueue(queueOptions, callback) {
        callback(null, app.amqp.assertQueue(queueName, queueOptions));
      }

      function subscribe(ok, callback) {
        ok.then(function () {
          app.amqp.bindQueue(queueName, "amq.topic", queueName);
          app.amqp.bindQueue(queueName, "amq.direct", queueName);
        });

        ok = ok.then(function (queueResult) {
          console.log("connected to", queueName);

          app.amqp.consume(queueName, function (message) {
            var content;

            try {
              content = JSON.parse(message.content.toString());
            } catch (error) {
              console.error("error parsing AMQP message", message, message.content, "on queue", queueName, error);
              return onMessage(error);
            }
            
            onMessage(null, content, message, queueResult.messageCount);
          }, {
            noAck: false
          });
        }, function (error) {
          console.error("error connecting to", queueName, "queue", error);
        }, {
          noAck: false
        });
      }
    },

    connect: function (callback) {
      lib.ensureConnection(callback);

      return lib;
    },

    ensureConnection: function (callback) {
      if (app.amqp) return callback(null, app.amqp);

      var timeout = setTimeout(function () {
        console.log("A connection could not be made to RabbitMQ. Is it running?");
        process.exit(1);
      }, 10000);

      var connectionUrl = ["amqp://"];

      if (amqpSettings.login) {
        connectionUrl.push(amqpSettings.login);

        if (amqpSettings.password) connectionUrl.push.call(connectionUrl, ":", amqpSettings.password);

        connectionUrl.push("@");
      }

      connectionUrl.push.call(connectionUrl, amqpSettings.host, ":", amqpSettings.port, "/", amqpSettings.vhost);

      connectionUrl = connectionUrl.join("");
      console.log("connecting to AMQP url", connectionUrl);

      amqp.connect(connectionUrl).then(function (connection) {
        return when(connection.createConfirmChannel().then(function (channel) {
          // we're actually going to store and re-use the channel
          // (which are multiplexed over TCP), not the connection
          // (which is TCP)
          app.amqp = channel;

          clearTimeout(timeout);
          console.log("connected to AMQP", amqpSettings);
          callback(null, app.amqp);
        }));
      }).then(null, console.warn);
    }
  };

  /**
   * Composes a standardized message with a meta key and a body
   * @param   {Object} The message object with an optional meta key
   * @returns {Object} message composed with meta key and body
   */
  function composeMessage(message) {
    message = message || {};

    var meta = message.meta || {};
    delete(message.meta);

    return new Buffer(
      JSON.stringify({
        meta: meta,
        body: message
      }),
      "utf-8"
    );
  }

  app.amqpHelpers = lib;
  return lib;
}