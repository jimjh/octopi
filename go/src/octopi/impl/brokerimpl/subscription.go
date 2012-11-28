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
	topic  string          // subscription topic
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
		topic:  topic,
		conn:   conn,
		log:    log,
		quit:   make(chan interface{}, 1),
	}, nil

}

// Serve blocks until either the websocket connection is closed, or until a
// message is received on the `quit` channel. This method may only be invoked
// once; after it returns, the subscription is closed.
func (s *Subscription) Serve() error {

	read := make(chan *LogEntry)
	go s.readFile(read)

	var err error
	for {
		select {
		case <-s.quit:
			break
		case entry := <-read:

			if nil == entry {
				break
			}

			err = websocket.JSON.Send(s.conn, &entry.Message)
			if nil != err {
				log.Error("Unable to send message to consumer: ", err.Error())
				break
			}

		}
	}

	log.Debug("Stopped serving subscription %p.", s)
	s.log.Close()

	return err

}

// readFile reads from the log file, sending log entries through the given
// channel. When it reaches the end of the file, it registers a conditional
// variable with the broker and waits for new messages.
func (s *Subscription) readFile(read chan<- *LogEntry) {

	for {

		entry, err := s.log.ReadNext()

		switch err {

		case nil:
			read <- entry

		case io.EOF: // wait for ping
			log.Warn("Reached end of log.")
			s.broker.wait(s)

		default:
			log.Error("Error reading from log: ", err.Error())
			read <- nil // give up on corrupted log
			break

		}

	}

}
