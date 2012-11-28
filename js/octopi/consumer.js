/* ========================================================================
 * consumer.js
 * http://github.com/jimjh/octopi
 * ========================================================================
 * Copyright (c) 2012 Carnegie Mellon University
 * License: https://raw.github.com/jimjh/octopi/master/LICENSE
 * ========================================================================
 */
/*jshint strict:true unused:true*/
/*global _ window*/

// ## octopi-consumer module
// Lets you create consumers and subscribe to websocket topics.
define(['./util', './config', './protocol'],
       function(util, config, protocol) {

  'use strict';

  // ### Consumer
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
  Consumer.prototype.subscribe = function(topic, callback, offset) {

    if (_.isEmpty(topic) || !_.isString(topic))
      throw new TypeError('Invalid topic. Should be a non-empty string.');
    if (!_.isFunction(callback))
      throw new TypeError('Invalid callback. Should be a function.');
    if (_.isUndefined(offset)) {
      offset = -1;
    }

    if (topic in this.subscriptions) return; // already subscribed

    var conn = this.subscriptions[topic] = new window.WebSocket(this.endpoint);

    conn.onmessage = handle(conn, callback);
    conn.onopen = function() {
      // if unable to send, just close and resubscribe
      if (!conn.send(protocol.subscription(topic, offset))) return conn.close();
      util.warn('Retrying connection to broker ...');
      // otherwise, wait for ACK
      var wait = _.random(config.max_retry_interval);
      conn.ack = window.setTimeout(conn.onopen, wait);
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
  var handle = function(conn, callback) {

    var onack = function(event) {
      util.log('Ack received from broker.');
      var ack = protocol.ack(event.data);
      if (ack.Status !== protocol.SUCCESS) return;
      window.clearTimeout(conn.ack); // stop retrying
      delete conn.ack;
    };

    var ondata = function(event) {
      var message = protocol.message(event.data);
      var checksum = protocol.checksum(message);
      // TODO: fix checksum issues for special characters
      if (true || checksum == message.Checksum) return callback(message.Payload);
      throw new Error('Incorrect checksum. Expected ' + checksum + ', was ' + message.Checksum);
      // TODO: checksum error
    };

    return function(event) {
      if (!_.isUndefined(conn.ack)) return onack(event);
      return ondata(event);
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

  return Consumer;

});
