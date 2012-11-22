// The file defines the protocol that producers, consumers, brokers, and
// registers will use to communicate with each other.
// TODO: versioning.
package protocol

import (
	"code.google.com/p/go.net/websocket"
	"container/list"
)

// Constants for contacting register.
const (
	BROKER = iota
	PRODUCER
	CONSUMER
)

// Constants for register to tell which role a broker is taking.
const (
	LEADER = iota
	FOLLOWER
)

type FollowWSConn struct{
	FollowWS *websocket.Conn
	Block chan interface{}
}

// Used by brokers to contact register.
type BrokerRegInit struct {
	Source   int
	HostPort string
}

/* used by register to assign brokers */
type RegBrokerInit struct {
	Role       int
	LeadUrl    string     //url of the leader, for followers
	LeadOrigin string     //origin of the leader, for followers
	Brokers    *list.List //list of associated brokers used if new leader
}

/* used by producers to contact register */
type ProdRegInit struct {
	Source int
	Topic  string
}

/* used by register to tell producer the lead broker */
type RegProdInit struct {
	LeadUrl    string
	LeadOrigin string
}

/* used by register to tell consumer its assigned broker */
type RegConsInit struct {
	BrokerUrl    string
	BrokerOrigin string
}

/* used by followers to contact leader */
type FollowLeadInit struct {
	Source   int
	HostPort string
	// TODO: other fields to identify position of log
}

// PublishRequests are sent from producers to brokers when they want to start
// sending messages under a particular topic.
type PublishRequest struct {
	Source int
	Topic  string
	// TODO: acknowledgments?
}

// SubscribeRequests are sent from consumers to brokers when they want messages
// from a particular topic.
type SubscribeRequest struct {
	Source int // XXX: should use inheritance here.
	Topic  string
	// TODO: other fields to identify position of wanted
	// TODO: acknowledgements
}

// Messages sent from producers to brokers; the enclosed payload is broadcast to
// all consumers subscribing to the topic.
type Message struct {
	ID       uint32 // seq num from producer, or offset from broker
	Payload  []byte // message contents
	Checksum uint32 // crc32 checksum
}
