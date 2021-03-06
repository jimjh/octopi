/* ========================================================================
 * protocol.js
 * http://github.com/jimjh/octopi
 * ========================================================================
 * Copyright (c) 2012 Carnegie Mellon University
 * License: https://raw.github.com/jimjh/octopi/master/LICENSE
 * ========================================================================
 */
/*jshint strict:true unused:true*/
/*global base64 crc32 window*/

define(function() {

  'use strict';

  var unicode = function(s) {
    return window.decodeURIComponent(window.escape(s));
  };

  return {

    // Endpoint for subscription requests.
    PATH: 'subscribe',

    // StatusSuccess
    SUCCESS: 200,

    // StatusRedirect
    REDIRECT: 320,

    // Creates a new subscription request for the given topic.
    subscription: function(topic, offset) {
      return JSON.stringify({Topic: topic, Offset: offset});
    },

    // Parses received ACK into a javascript object.
    ack: function(string) {
      var obj = JSON.parse(string);
      obj.Payload = base64.decode(obj.Payload);
      return obj;
    },

    unicode: unicode,

    // Parses received message into a javascript object.
    message: function(string) {
      var obj = JSON.parse(string);
      obj.Payload = base64.decode(obj.Payload);
      obj.Length = obj.Payload.length;
      return obj;
    },

    // Calculates the checksum of the message's payload.
    checksum: function(message) {
      return crc32(unicode(message.Payload));
    }

  };

});
