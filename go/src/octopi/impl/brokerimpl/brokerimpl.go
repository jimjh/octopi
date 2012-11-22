package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"container/list"
	"octopi/api/protocol"
	"sync"
)

type Broker struct {
	role          int                        // role of the broker
	brokerConns   map[string]*protocol.FollowWSConn // map of assoc broker hostport to connections, for leaders
	subscriptions map[string]*list.List      // map of topics to consumer connections
	leadUrl       string                     // url of the leader, for followers
	leadOrigin    string                     // origin of the leader, for followers
	leadConn      *websocket.Conn            // connection to the leader, for followers
	lock          sync.Mutex                 //  lock to manage broker access
}

func NewBroker(rbi protocol.RegBrokerInit, regconn *websocket.Conn) (*Broker, error) {

	/* create the broker */
	b := &Broker{
		role:          rbi.Role,
		subscriptions: make(map[string]*list.List),
		leadUrl:       rbi.LeadUrl,
		leadOrigin:    rbi.LeadOrigin,
	}

	// XXX: topics are freely created - this is pub/sub convention.

	/* only initialize these variables if leader. otherwise leave as nil */
	if rbi.Role == protocol.LEADER {
		b.brokerConns = make(map[string]*protocol.FollowWSConn)

		// TODO: make websocket connection to brokers. use rbi.Brokers
	} else {
		leadConn, err := websocket.Dial(rbi.LeadUrl, "", rbi.LeadOrigin)
		if nil != err {
			return nil, err
		}
		b.leadConn = leadConn
	}

	return b, nil
}

// RegisterProducer creates a new publication for the given producer connection
// and blocks until the connection is lost.
func (b *Broker) RegisterProducer(conn *websocket.Conn, req *protocol.PublishRequest) error {
	publication := NewPublication(conn, req.Topic, b)
	return publication.Serve()
}

// Publish publishes the given message to all subscribers.
func (b *Broker) Publish(topic string, msg *protocol.Message) {

	// FollowBroadcast
	// TODO: this is not a good idea (should be async.)
	// XXX: before writing to file, make sure to replace ID with offset and check
	// for duplicates.

	b.lock.Lock()
	defer b.lock.Unlock()

	subscriptions, exists := b.subscriptions[topic]
	if !exists { // no subscribers
		return
	}

	for ele := subscriptions.Front(); ele != nil; ele = ele.Next() {
		subscription := ele.Value.(*Subscription)
		subscription.send <- msg
	}

}

func (b *Broker) FollowBroadcast(msg protocol.Message) {
	b.lock.Lock()
	defer b.lock.Unlock()
	/* loop through all follwing broker connections and send message to update */
	for hostport, conn := range b.brokerConns {
		go b.sendToFollow(hostport, conn, msg)
	}
}

func (b *Broker) sendToFollow(hostport string, ws *protocol.FollowWSConn, msg protocol.ProduceRequest) {
	err := websocket.JSON.Send(ws.FollowWS, msg)
	if nil != err {
		b.lock.Lock()
		defer b.lock.Unlock()
		delete(b.brokerConns, hostport)
		ws.FollowWS.Close()
		/* allows websocket handler */
		ws.Block<-nil
	}
}

func (b *Broker) CacheFollower(hostport string, fconn *protocol.FollowWSConn){
	b.lock.Lock()
	defer b.lock.Unlock()
	b.brokerConns[hostport] = fconn
}

// RegisterConsumer creates a new subscription for the given consumer.
//
// Consumers are allowed to register for non-existent topics, but will not
// receive any messages until a producer publishes a message under that topic.
//
// This method blocks until the websocket connection is broken.
func (b *Broker) RegisterConsumer(conn *websocket.Conn, req *protocol.SubscribeRequest) error {

	// create new subscription
	subscription := NewSubscription(conn)
	var subscriptions *list.List

	b.lock.Lock()

	// save subscription (XXX: what if the same consumer subscribes twice?)
	subscriptions, exists := b.subscriptions[req.Topic]
	if !exists {
		b.subscriptions[req.Topic] = list.New()
		subscriptions = b.subscriptions[req.Topic]
	}
	subscriptions.PushBack(subscription)

	b.lock.Unlock()

	// serve (blocking call)
	subscription.Serve()

	return nil

}

func (b *Broker) RegBroker(ws *websocket.Conn, fli protocol.FollowLeadInit) {
	b.lock.Lock()
	defer b.lock.Unlock()

}

func (b *Broker) Role() int {
	return b.role
}
