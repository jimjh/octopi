package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
)

// Subscriptions are used to store consumer connections; each subscription has
// a go channel that relays messages to the consumer.
type Subscription struct {
	conn *websocket.Conn  // consumer websocket connection
	send chan interface{} // go channel for relaying messages
}

// NewSubscription creates a new subscription.
func NewSubscription(conn *websocket.Conn) *Subscription {
	return &Subscription{
		conn: conn,
		send: make(chan interface{}, 1), // TODO: make this more robust
	}
}

// Serve blocks until either the websocket connection or channel is closed.
func (s *Subscription) Serve() {
	for message := range s.send {
		if nil != websocket.JSON.Send(s.conn, message) {
			break
		}
	}
}
