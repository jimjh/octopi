// # Octopi Consumer Library
// Lets you create consumers and subscribe to websocket topics.
//
//      var c = new Consumer('localhost:12345');
//      c.subscribe('topic', function(msg) {
//        console.log(msg);
//      });
//
/*global WebSocket*/

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
      return JSON.parse(string);
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
  };

  // Subscribes to the given topic, invoking the callback with the received
  // message.
  //
  //      c.subscribe('topic', function() { /* ... */ });
  //
  Consumer.prototype.subscribe = function(topic, callback) {

    if (typeof topic === 'undefined') throw new Error('Invalid topic.');
    if (!callback) throw new Error('Invalid callback.');

    var ws = new WebSocket(this.endpoint);
    ws.onmessage = handle(callback);
    ws.onopen = function() { ws.send(protocol.subscription(topic)); };

  };

  // Returns a function that handles incoming websocket messages and passes
  // them to the user-supplied callback.
  var handle = function(callback) {
    return function(event) {

      var message = protocol.message(event.data);

      var checksum = protocol.checksum(message);
      if (checksum == message.Checksum) return callback(message.Payload);

      throw new Error('Incorrect checksum. Expected ' + checksum + ', was ' + message.Checksum);

      // TODO: sequence guarantees, and reconnect on checksum failure

    };
  };

  o.protocol = protocol;
  o.Consumer = Consumer;

})(octopi);
