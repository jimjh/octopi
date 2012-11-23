package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"io"
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
func (s *Subscription) Serve() error {

	var err error
	for message := range s.send {
		if err = websocket.JSON.Send(s.conn, message); nil != err {
			break
		}
	}

	if err == io.EOF {
		return nil
	}

	return err

}
