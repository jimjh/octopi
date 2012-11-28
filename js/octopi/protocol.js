/* ========================================================================
 * protocol.js
 * http://github.com/jimjh/octopi
 * ========================================================================
 * Copyright (c) 2012 Carnegie Mellon University
 * License: https://raw.github.com/jimjh/octopi/master/LICENSE
 * ========================================================================
 */
/*jshint strict:true unused:true*/
/*global base64 crc32*/

define(function() {

  'use strict';

  return {

    // Endpoint for subscription requests.
    PATH: 'subscribe',

    // Status codes
    SUCCESS: 200,

    // Creates a new subscription request for the given topic.
    subscription: function(topic, offset) {
      return JSON.stringify({Topic: topic, Offset: offset});
    },

    // Parses received ACK into a javascript object.
    ack: function(string) {
      return JSON.parse(string);
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

});
