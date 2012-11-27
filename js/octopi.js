/* ========================================================================
 * octopi.js
 * http://github.com/jimjh/octopi
 * ========================================================================
 * Copyright (c) 2012 Carnegie Mellon University
 * License: https://raw.github.com/jimjh/octopi/master/LICENSE
 * ========================================================================
 */
/*jshint strict:true unused:true*/

// # Octopi Client
// Provides a javascript consumer for applications that require
// publish/subscribe service.
//
// See main.js for an example of how to include this in your app.
//
// ## Interface
// To create a new consumer and subscribe to a topic, do the following:
//
//      var c = new Consumer('localhost:12345');
//      c.subscribe('topic', function(msg) {
//        console.log(msg);
//      });
//
//} TODO: combine crc32 and base64 into modules
define(['octopi/config', 'octopi/protocol', 'octopi/consumer'],
       function(config, protocol, consumer) {

   'use strict';

   return {
     config: config,
     protocol: protocol,
     Consumer: consumer
   };

});
