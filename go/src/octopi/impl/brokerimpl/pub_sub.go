package brokerimpl

// This file contains the publish and subscribe functions.
import (
	"code.google.com/p/go.net/websocket"
	"octopi/api/protocol"
	"octopi/util/log"
	"sync"
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

	file, err := b.getOrOpenLog(topic)
	if nil != err {
		return err
	}

	entry, err := file.Append(producer, msg)
	if nil != err {
		return err
	}

	b.replicate(topic, entry)

	b.cond.Broadcast()
	return nil

}

// replicate  replicates the given log entry across all followers.
func (b *Broker) replicate(topic string, entry *LogEntry) error {

	// send message to all followers
	for follower := range b.followers {
		go func() {
			sync := &protocol.Sync{topic, entry.Message, entry.RequestId}
			if err := websocket.JSON.Send(follower.conn, sync); nil != err {
				// lost
				b.removeFollower(follower)
			}
		}()
	}

	var group sync.WaitGroup

	// wait for ACK
	for follower := range b.followers {
		group.Add(1)
		go func() {
			defer group.Done()
			var ack protocol.SyncACK
			if err := websocket.JSON.Receive(follower.conn, &ack); nil != err {
				// lost
				b.removeFollower(follower)
			}
		}()
	}

	group.Wait()
	return nil

}

// removeFollower disconnects follower from followers set.
func (b *Broker) removeFollower(follower *Follower) {

	_, exists := b.followers[follower]
	if !exists {
		return
	}

	delete(b.followers, follower)
	follower.quit <- nil

	log.Info("Removed follower %p from follower set.", follower)

}