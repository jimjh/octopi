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
    this.register = host;
    this.endpoint = 'ws://' + host + '/' + protocol.PATH;
    this.subscriptions = {};
  };

  // Subscribes to the given topic, invoking the callback with the received
  // message.
  //
  //      c.subscribe('topic', function() { /* ... */ });
  //
  // If a subscription already exists for the given topic, it will be ignored.
  Consumer.prototype.subscribe = function(topic, callback, offset) {

    if (_.isEmpty(topic) || !_.isString(topic))
      throw new TypeError('Invalid topic. Should be a non-empty string.');
    if (!_.isFunction(callback))
      throw new TypeError('Invalid callback. Should be a function.');
    if (_.isUndefined(offset)) {
      offset = -1;
    }

    if (topic in this.subscriptions) return; // already subscribed

    var conn = new window.WebSocket(this.endpoint);
    var subscription =
      this.subscriptions[topic] = { topic: topic, conn: conn, offset: offset };

    conn.onmessage = handle(this, subscription, callback);
    conn.onopen = function() {
      // if unable to send, just close and resubscribe
      if (!conn.send(protocol.subscription(topic, offset))) return conn.close();
      util.warn('Retrying connection to broker ...');
      // otherwise, wait for ACK
      var wait = _.random(config.max_retry_interval);
      subscription.ack = window.setTimeout(conn.onopen, wait);
    };

    conn.onclose = _.bind(onclose, this, conn, topic, callback);

  };

  // Unsubscribes from the given topic by closing the websocket connection.
  // This is a no-op if a subscription does not already exist for the topic.
  Consumer.prototype.unsubscribe = function(topic) {

    if (_.isEmpty(topic) || !_.isString(topic))
      throw new TypeError('Invalid topic. Should be a non-empty string.');
    if (!(topic in this.subscriptions)) return;

    var conn = this.subscriptions[topic].conn;
    delete this.subscriptions[topic];
    conn.close();

  };

  // Resubscribes to the given topic. This is a private method. Use `subscribe`
  // and `unsubscribe` instead.
  Consumer.prototype.resubscribe = function(topic, callback) {

    var that = this;
    var wait = _.random(config.max_retry_interval);

    window.setTimeout(function() {
      var next = that.subscriptions[topic].offset;
      delete that.subscriptions[topic];
      that.subscribe(topic, callback, next);
    }, wait);

  };

  // Returns a function that handles incoming websocket messages and passes
  // them to the user-supplied callback.
  var handle = function(consumer, subscription, callback) {

    var clear = function() {
      window.clearTimeout(subscription.ack);
      delete subscription.ack;
    };

    var onack = function(event) {

      var ack = protocol.ack(event.data);
      switch(ack.Status) {
        case protocol.REDIRECT:
          clear();
          consumer.endpoint = "ws://" + ack.Payload + "/" + protocol.PATH;
          util.log('redirected to ' + consumer.endpoint);
          consumer.unsubscribe(subscription.topic);
          consumer.subscribe(subscription.topic, callback, subscription.offset);
          break;
        case protocol.SUCCESS:
          clear();
          break;
      }

    };

    var ondata = function(event) {
      var message = protocol.message(event.data);
      var checksum = protocol.checksum(message);
      // TODO: fix checksum issues for special characters
      subscription.offset += message.Length + 40;
      if (true || checksum == message.Checksum) return callback(protocol.unicode(message.Payload));
      throw new Error('Incorrect checksum. Expected ' + checksum + ', was ' + message.Checksum);
    };

    return function(event) {
      if (!_.isUndefined(subscription.ack)) return onack(event);
      return ondata(event);
    };

  };

  // Invoked when a websocket connection is closed.
  var onclose = function(conn, topic, cb) {

    // do nothing if intentional close, or different connection
    if (!(topic in this.subscriptions && this.subscriptions[topic].conn === conn))
      return;

    // back off and reconnect
    this.endpoint = "ws://" + this.register + "/" + protocol.PATH;
    this.resubscribe(topic, cb);

  };

  return Consumer;

});
