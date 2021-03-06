// Package protocol defines the protocol that producers, consumers, brokers,
// and registers use to communicate with each other.
package protocol

// URL endpoints
const (
	// for brokers
	PUBLISH   = "publish"   // producer -> broker
	SUBSCRIBE = "subscribe" // consumer -> broker
	FOLLOW    = "follow"    // follower -> leader
	SWAP      = "swap"      // register -> broker
	// for register
	LEADER = "leader" // leader -> register
)

// Status codes
const (
	StatusSuccess  = 200 // successful operation
	StatusRedirect = 320 // redirect to attached host:port
	StatusNotReady = 350 // status is not ready (used in register)
	StatusFailure  = 400 // failed operation
)

// Register add or remove a follower
const (
	ADD = iota
	REMOVE
)

// FollowRequests are sent by brokers to registers/leaders when they wish to
// join the broker set.
type FollowRequest struct {
	Offsets  map[string]int64 // high watermarks of each topic log
	HostPort HostPort         // hostport of the follower
}

// FollowACKs are sent from leaders to followers in response to follow
// requests.
type FollowACK struct {
	Truncate map[string]int64
}

// Hostports are string representations of TCP addresses.
type HostPort string

// InsyncChanges are used by Leaders to contact the register whether
// to add or remove a hostport from the list of in-sync followers
type InsyncChange struct {
	Type     int
	HostPort HostPort
}

// Syncs are sent from leaders to followers.
type Sync struct {
	Topic     string  // topic
	Message   Message // message
	RequestId []byte  // sha256 of producer seqnum
}

// SyncACKs are sent from followers to leaders after receiving sync messages
// from leader.
type SyncACK struct {
	Topic  string
	Offset int64 // offset of last message received
}

// ACKs are sent from registers/brokers to producers/consumers/brokers.
type Ack struct {
	Status  int    // status code
	Payload []byte // payload
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
type Message struct {
	ID       int64  // seq num from producer, or offset from broker
	Payload  []byte // message contents
	Checksum uint32 // crc32 checksum
}
