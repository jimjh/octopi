// Package protocol defines the protocol that producers, consumers, brokers,
// and registers will use to communicate with each other.
// TODO: versioning.
package protocol

import (
	"code.google.com/p/go.net/websocket"
)

// URL endpoints
const (
	PUBLISH   = "publish"   // producer -> broker
	SUBSCRIBE = "subscribe" // consumer -> broker
	FOLLOW    = "follow"
)

// Status codes
const (
	SUCCESS  = 200 // successful operation
	REDIRECT = 320 // redirect to attached host:port
	NOTREADY = 350 // status is not ready (used in register)
	FAILURE  = 400 // failed operation
)

// Sync types
const (
	UPDATE = iota
	COMMIT
)

// Register add or remove a follower
const (
	ADD = iota
	REMOVE
)

// Max number of milliseconds between retries.
// TODO: move this to a configuration file
var MAX_RETRY_INTERVAL = 2000

type Follower struct {
	Conn *websocket.Conn
}

// FollowRequests are sent by brokers to registers/leaders.
type FollowRequest struct {
	Offsets map[string]int64 // high watermarks of each topic log
}

// FollowACKs are sent from leaders to followers in response to follow
// requests.
type FollowACK struct {
}

// Hostports are string representations of hostports
type Hostport string

// InsyncChanges are used by Leaders to contact the register whether
// to add or remove a hostport from the list of in-sync followers
type InsyncChange struct {
	Type     int
	Hostport Hostport
}

// SyncRequests are sent from leaders to followers asking them to write
// to log.
type SyncRequest struct {
	Type    int
	Topic   string
	Message []byte //can be the highwatermark if used as commit
}

// SyncACKs are sent from followers to leaders after receiving sync messages
// from leader.
type SyncACK struct {
	Topic  string
	Offset int64 // offset of last message received
}

// ACKs are sent from registers/brokers to producers/consumers/brokers.
type Ack struct {
	Status   int    // status code
	HostPort string // optional redirect
}

// ProduceRequests are sent from producers to brokers when they want to send
// messages under a specific topic.
type ProduceRequest struct {
	ID      string // id of producer
	Topic   string
	Message Message
}

// SubscribeRequests are sent from consumers to brokers when they want messages
// from a particular topic.
type SubscribeRequest struct {
	Topic  string
	Offset int64 // optional
}

// Messages sent from producers to brokers; the enclosed payload is broadcast
// to all consumers subscribing to the topic.
// XXX: do we need ID?
type Message struct {
	ID       int64  // seq num from producer, or offset from broker
	Payload  []byte // message contents
	Checksum uint32 // crc32 checksum
}
