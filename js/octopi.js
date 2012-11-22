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
    //} TODO: replace with msgpack.
    subscription: function(topic) {
      return JSON.stringify({MessageSrc: protocol.CONSUMER, Topic: topic});
    }

  };

  // ## Consumer
  var Consumer = function(host) {
    if (!host) throw new Error('Invalid host.');
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
      callback(event.data);
    };
  };

  o.protocol = protocol;
  o.Consumer = Consumer;

})(octopi);
