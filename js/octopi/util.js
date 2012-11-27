/* ========================================================================
 * util.js
 * http://github.com/jimjh/octopi
 * ========================================================================
 * Copyright (c) 2012 Carnegie Mellon University
 * License: https://raw.github.com/jimjh/octopi/master/LICENSE
 * ========================================================================
 */
/*jshint strict:true unused:true*/
/*global _ console window*/

// ## octopi-util module
// Provides utility functions.
define(function() {

  'use strict';

  var util = {

    // Logs a message if `console.log` is available.
    //
    //      util.log('my log message.');
    log: function() {
      var no_logger = (_.isUndefined(window.console) || _.isUndefined(console.log));
      if (!no_logger) _.each(arguments, function(m) { console.log(m); });
    },

    // Logs a warning message if `console.warn` is available.
    //
    //      util.warn('my warning message.');
    warn: function() {
      var no_warner = (_.isUndefined(window.console) || _.isUndefined(console.warn));
      if (!no_warner) _.each(arguments, function(w){ console.warn(w); });
      else util.log.apply(this, arguments);
    }

  };

  return util;

});
