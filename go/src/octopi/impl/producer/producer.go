package producer

import (
	"code.google.com/p/go.net/websocket"
	crand "crypto/rand"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/big"
	"math/rand"
	"octopi/api/protocol"
	"octopi/util/log"
	"sync"
	"sync/atomic"
	"time"
)

// Producers publish messages to brokers.
type Producer struct {
	conn   *websocket.Conn // persistent websocket connection
	seqnum int64           // sequence number of messages
	lock   sync.Mutex      // lock for producer state
	id     string          // producer ID
}

// Default origin to use.
var ORIGIN = "http://localhost"

// Max number of retries.
// XXX: move this to a configuration file
var MAX_RETRIES = 5

func seed() {
	randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(randint.Int64())
}

// New creates a new producer that sends messages to broker that lives
// at the given hostport.
func New(hostport string, id *string) (*Producer, error) {

	if nil == id {
		seed()
		idStr := fmt.Sprintf("%d", rand.Uint32())
		id = &idStr
	}

	p := &Producer{id: *id}
	addr := "ws://" + hostport + "/" + protocol.PUBLISH

	if err := p.connect(addr); nil != err {
		return nil, err
	}

	return p, nil

}

// Establishes a websocket connection to the broker.
func (p *Producer) connect(addr string) error {

	conn, err := websocket.Dial(addr, "", ORIGIN)

	if nil != err {
		log.Error(err.Error())
	} else {
		p.conn = conn
		log.Info("Established new connection to broker at %v.", conn.RemoteAddr())
	}

	return err

}

// Locks down producer, closes current connection, and reconnects to broker.
func (p *Producer) reconnect() error {

	p.lock.Lock()
	defer p.lock.Unlock()

	p.conn.Close()

	// back off and reconnect
	backoff := time.Duration(rand.Intn(protocol.MAX_RETRY_INTERVAL))
	time.Sleep(backoff * time.Millisecond)
	return p.connect(p.conn.RemoteAddr().String())

}

// Send sends the message to the broker, and blocks until an acknowledgement is
// received. If the max number of retries is exceeded, returns the last error.
func (p *Producer) Send(topic string, payload []byte) error {

	seqnum := atomic.AddInt64(&p.seqnum, 1)
	message := protocol.Message{seqnum, payload, crc32.ChecksumIEEE(payload)}
	request := &protocol.ProduceRequest{p.id, topic, message}

	log.Debug("Sending %v", request)

	var err error
	for tries := 1; tries <= MAX_RETRIES; tries++ {

		err = websocket.JSON.Send(p.conn, request)
		if nil == err { // wait for ACK
			timeout := time.Duration(protocol.MAX_RETRY_INTERVAL)
			select {
			case ack, ok := <-p.receiveAck():
				if ok {
					if protocol.SUCCESS == ack.Status {
						log.Debug("Ack received.")
						break // success
					}
					continue
				} // otherwise, reconnect
				err = &ProduceError{seqnum}
			case <-time.After(timeout * time.Millisecond):
				log.Debug("Timed out waiting for ACK ...")
				err = &ProduceError{seqnum}
				continue
			}
		}

		log.Error(err.Error())
		p.reconnect()
		log.Debug("Retrying...")

	}

	return err

}

// receiveAck waits for an acknowledgement from the broker.
func (p *Producer) receiveAck() <-chan *protocol.Ack {

	ackc := make(chan *protocol.Ack)
	go func() {
		for {
			var ack protocol.Ack
			err := websocket.JSON.Receive(p.conn, &ack)
			if nil == err {
				ackc <- &ack
				break
			}
			if io.EOF == err {
				close(ackc)
				break
			}
		}
	}()

	return ackc

}

// Close closes the producer's websocket connection. Must not be invoked while
// there are still Sends pending.
func (p *Producer) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.conn.Close()
}

// ProduceErrors indicate that a produce request failed.
type ProduceError struct {
	seqnum int64
}

func (e *ProduceError) Error() string {
	return fmt.Sprintf("Acknowledgement for message %d was not received.", e.seqnum)
}
