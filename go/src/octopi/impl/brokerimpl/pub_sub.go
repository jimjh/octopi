package brokerimpl

// This file contains the publish and subscribe functions.
import (
	"code.google.com/p/go.net/websocket"
	"octopi/api/protocol"
)

// Subscribe creates a new subscription for the given consumer connection.
// Consumers are allowed to register for non-existent topics, but will not
// receive any messages until a producer publishes a message under that topic.
func (b *Broker) Subscribe(
	conn *websocket.Conn,
	topic string,
	offset int64) (*Subscription, error) {

	// create new subscription
	subscription, err := NewSubscription(b, conn, topic, offset)
	if nil != err {
		return nil, err
	}

	var subscriptions SubscriptionSet

	b.lock.Lock()
	defer b.lock.Unlock()

	// save subscription
	subscriptions, exists := b.subscriptions[topic]
	if !exists {
		subscriptions = make(map[*Subscription]bool)
		b.subscriptions[topic] = subscriptions
	}
	subscriptions[subscription] = true

	return subscription, nil

}

// Unsubscribe removes the given subscription from the broker.
func (b *Broker) Unsubscribe(topic string, subscription *Subscription) {

	b.lock.Lock()
	defer b.lock.Unlock()

	subscriptions, exists := b.subscriptions[topic]
	if !exists {
		return
	}

	subscription.quit <- nil
	delete(subscriptions, subscription)

}

// wait checks if the subscription is really at the end of the log. It returns
// iff there is more to be read.
func (b *Broker) wait(s *Subscription) {

	b.lock.Lock()
	defer b.lock.Unlock()

	for s.log.IsEOF() {
		b.cond.Wait()
		return
	}

}

// Publish publishes the given message to all subscribers.
func (b *Broker) Publish(topic, producer string, msg *protocol.Message) error {

	// TODO: topic-specific locks
	b.lock.Lock()
	defer b.lock.Unlock()

	var err error
	file, exists := b.logs[topic]
	if !exists {
		file, err = OpenLog(b.config, topic, -1)
		if nil != err {
			return err
		} else {
			b.logs[topic] = file
		}
	}

	err = file.Append(producer, msg)
	if nil != err {
		return err
	}

	b.cond.Broadcast()
	return nil

}
