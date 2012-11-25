// # Octopi Consumer Library
// Lets you create consumers and subscribe to websocket topics.
//
//      var c = new Consumer('localhost:12345');
//      c.subscribe('topic', function(msg) {
//        console.log(msg);
//      });
//
/*global window crc32 base64 _*/

// ## Global octopi object
// All octopi functions will be enclosed in this namespace.
var octopi = octopi || {};

(function(o) {

  'use strict';

  // ## Protocol
  var protocol = {

    // Endpoint for subscription requests.
    PATH: 'subscribe',

    // Node type identifier.
    CONSUMER: 2,

    // Max number of milliseconds between retries.
    MAX_RETRY_INTERVAL: 2000,

    // Creates a new subscription request for the given topic.
    subscription: function(topic) {
      return JSON.stringify({Source: protocol.CONSUMER, Topic: topic});
    },

    // Parses received message into a javascript object.
    message: function(string) {
      var obj = JSON.parse(string);
      obj.Payload = base64.decode(obj.Payload);
      return obj;
    },

    // Calculates the checksum of the message's payload.
    checksum: function(message) {
      return crc32(message.Payload);
    }

  };

  // ## Consumer
  // `c.endpoint`: the endpoint this consumer will send requests to.
  // `c.subscriptions`: map from topic to websocket connection.
  var Consumer = function(host) {
    if (_.isEmpty(host) || !_.isString(host))
      throw new TypeError('Invalid host. It should look like example.com:123.');
    this.endpoint = 'ws://' + host + '/' + protocol.PATH;
    this.subscriptions = {};
  };

  // Subscribes to the given topic, invoking the callback with the received
  // message.
  //
  //      c.subscribe('topic', function() { /* ... */ });
  //
  // If a subscription already exists for the given topic, it will be ignored.
  //} TODO: how to let application know if subscription failed?
  Consumer.prototype.subscribe = function(topic, callback) {

    if (_.isEmpty(topic) || !_.isString(topic))
      throw new TypeError('Invalid topic. Should be a non-empty string.');
    if (!_.isFunction(callback))
      throw new TypeError('Invalid callback. Should be a function.');

    if (topic in this.subscriptions) return; // already subscribed

    var conn = this.subscriptions[topic] = new window.WebSocket(this.endpoint);

    conn.onmessage = handle(callback);
    conn.onopen = function() {
      // TODO: wait for ACK; timeout
      // TODO: close connection on failure
      conn.send(protocol.subscription(topic));
    };

    conn.onclose = _.bind(onclose, this, conn, topic, callback);

  };

  // Unsubscribes from the given topic by closing the websocket connection.
  // This is a no-op if a subscription does not already exist for the topic.
  Consumer.prototype.unsubscribe = function(topic) {

    if (_.isEmpty(topic) || !_.isString(topic))
      throw new TypeError('Invalid topic. Should be a non-empty string.');
    if (!(topic in this.subscriptions)) return;

    var conn = this.subscriptions[topic];
    delete this.subscriptions[topic];
    conn.close();

  };

  // Resubscribes to the given topic. This is a private method. Use `subscribe`
  // and `unsubscribe` instead.
  Consumer.prototype.resubscribe = function(topic, callback) {

    var that = this;
    var wait = _.random(protocol.MAX_RETRY_INTERVAL);

    // TODO: use last offset
    // TODO: should we give up after a certain number of retries?
    window.setTimeout(function() {
      delete that.subscriptions[topic];
      that.subscribe(topic, callback);
    }, wait);

  };

  // Returns a function that handles incoming websocket messages and passes
  // them to the user-supplied callback.
  var handle = function(callback) {
    return function(event) {

      var message = protocol.message(event.data);

      var checksum = protocol.checksum(message);
      if (checksum == message.Checksum) return callback(message.Payload);

      console.log(message);
      throw new Error('Incorrect checksum. Expected ' + checksum + ', was ' + message.Checksum);

      // TODO: sequence guarantees, and rewind on checksum failure

    };
  };

  // Invoked when a websocket connection is closed.
  var onclose = function(conn, topic, cb) {

    // do nothing if intentional close, or different connection
    if (!(topic in this.subscriptions && this.subscriptions[topic] === conn))
      return;

    // back off and reconnect
    this.resubscribe(topic, cb);

  };

  o.protocol = protocol;
  o.Consumer = Consumer;

})(octopi);
