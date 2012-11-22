package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"octopi/api/protocol"
)

// Publications are used to store producer connections; each publication has a
// go channel that relays messages from the producer.
//
// XXX: currently, we do not check if the topics are the same as the one it
// registered with.
//
// XXX: for now, a publication is defined as a persistent connection for one
// more more produce requests. Should probably rename this.
//
// XXX: should producers be allowed to reuse the connection?
type Publication struct {
	conn    *websocket.Conn  // producer websocket connection
	broker  *Broker          // message broker
	receive chan interface{} // go channel for relaying messages
}

// NewPublication creates a new publication.
func NewPublication(conn *websocket.Conn, broker *Broker) *Publication {
	return &Publication{
		conn:    conn,
		broker:  broker,
		receive: make(chan interface{}, 1), // TODO: make this more robust
	}
}

// Serve blocks until the websocket connection is broken. Closes the `receive`
// channel when it returns.
// TODO: allow broker to shut down publications?
func (p *Publication) Serve() error {

	for {
		var request protocol.ProduceRequest
		err := websocket.JSON.Receive(p.conn, &request)
		if nil != err {
			break
		}
		p.broker.Publish(&request)
	}

	close(p.receive)
	return nil // XXX: what should be returned?

}
