package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"io"
	"octopi/util/log"
)

// Subscriptions are used to store consumer connections; each subscription has
// a go channel that relays messages to the consumer.
type Subscription struct {
	broker *Broker
	conn   *websocket.Conn // consumer websocket connection
	log    *Log            // broker log
	quit   chan interface{}
}

// NewSubscription creates a new subscription for the given topic. Messages are
// sent on the given connection.
func NewSubscription(
	broker *Broker,
	conn *websocket.Conn,
	topic string,
	offset int64) (*Subscription, error) {

	log, err := OpenLog(broker.config, topic, offset)
	if nil != err {
		return nil, err
	}

	return &Subscription{
		broker: broker,
		conn:   conn,
		log:    log,
		quit:   make(chan interface{}, 1),
	}, nil

}

// Serve blocks until either the websocket connection is closed, or until a
// message is received on the `quit` channel. This method may be invoked at
// most once; after it returns, the subscription is closed.
func (s *Subscription) Serve() error {

	defer s.log.Close()
	defer log.Debug("Stopped serving subscription %p.", s)

	for {
		select {
		case <-s.quit:
			return nil
		default:
			if err := s.next(); nil != err {
				log.Error("Unable to read from log: ", err.Error())
				return err
			}
		}
	}

	return nil

}

// next reads the next message from the associated log file. When it reaches
// the end of the file, it waits (using a conditional variable) for more
// messages from the broker. As a consequence of this design, a waiting
// subscription cannot be closed until a new message is published, waking it
// up. Returns nil or associated error.
func (s *Subscription) next() error {

	entry, err := s.log.ReadNext()

	switch err {
	case nil:
	case io.EOF: // wait for more
		log.Debug("Reached end of log.")
		s.broker.wait(s)
		return nil
	default: // abort
		return err
	}

	return websocket.JSON.Send(s.conn, &entry.Message)

}
