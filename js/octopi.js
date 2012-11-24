// # Octopi Consumer Library
// Lets you create consumers and subscribe to websocket topics.
//
//      var c = new Consumer('localhost:12345');
//      c.subscribe('topic', function(msg) {
//        console.log(msg);
//      });
//
/*global WebSocket crc32 base64*/

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
  var Consumer = function(host) {
    if (!host) throw new Error('Invalid host. It should look like example.com:123.');
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

    if (typeof topic === 'undefined') throw new Error('Invalid topic.');
    if (!callback) throw new Error('Invalid callback.');
    if (topic in this.subscriptions) return; // already subscribed

    var conn = new WebSocket(this.endpoint);
    this.subscriptions[topic] = conn;

    conn.onmessage = handle(callback);
    conn.onopen = function() {
      // TODO: wait for ACK
      conn.send(protocol.subscription(topic));
      // TODO: close connection on failure
      // TODO: detect broker failure
    };

    conn.onclose = function() {
      console.log('closed!');
    };

  };

  // Unsubscribes from the given topic by closing the websocket connection.
  // This is a no-op is a subscription does not already exist for the topic.
  Consumer.prototype.unsubscribe = function(topic) {
    if (topic in this.subscriptions) {
      var conn = this.subscriptions[topic];
      delete this.subscriptions[topic];
      conn.close();
    }
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

  o.protocol = protocol;
  o.Consumer = Consumer;

})(octopi);
