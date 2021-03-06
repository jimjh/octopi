package protocol

import (
	"code.google.com/p/go.net/websocket"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"octopi/util/log"
	"sync"
	"time"
)

// STYLE NOTE: Exported methods request for locks before they do anything. For
// example, `Close` gets the socket lock before calling `close`, which doesn't
// request for locks. This convention only applies to this file.

// Max number of milliseconds between retries.
const MAX_RETRY_INTERVAL = 2000

// ABORT is the error returned by Open if the maximum number of attempts has
// been exceeded.
var ABORT = errors.New("Exceeded maximum number of attempts.")

// Websocket protocol prefix
const ws = "ws://"

type Socket struct {
	HostPort string          // host:port of target node
	Path     string          // target url that is serving ws requests
	Origin   string          // source origin (See websockets spec)
	Conn     *websocket.Conn // websocket connection
	lock     sync.Mutex      // lock
}

// Reset resets the sockets HostPort to the given address. This closes the
// connections and interrupts any pending Sends.
func (s *Socket) Reset(addr string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.close()
	s.HostPort = addr
}

// Close closes the connection. This interrupts any pending Sends.
func (s *Socket) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.close()
}

func (s *Socket) close() error {
	conn := s.Conn
	s.Conn = nil
	if nil == conn {
		return nil
	}
	log.Info("Closed connection with %s.", conn.RemoteAddr())
	return conn.Close()
}

// Send sends the request to the given endpoint, and keeps trying until it
// succeeds or exceeds the maximum number of retries. If it encounters a
// redirect, the enclosed hostport is used as to find the new endpoint. Returns
// a channel that can be used to receive messages if there are no errors.
func (s *Socket) Send(request interface{}, attempts int, origin string) ([]byte, error) {

	s.lock.Lock()
	defer s.lock.Unlock()

	for attempt := 0; attempt < attempts; attempt++ {

		if s.HostPort == origin {
			return nil, fmt.Errorf("Should not dial yourself!")
		}

		endpoint := ws + s.HostPort + "/" + s.Path

		// open connection and send message
		err := s.send(endpoint, request)
		if nil != err {
			log.Warn("Unable to open connection with %s: %s", endpoint, err.Error())
			s.backoff()
			continue
		}

		// wait for acknowledgement
		var ack Ack
		err = s.receive(&ack)

		if nil == err {

			// interpret status
			switch ack.Status {
			case StatusFailure:
				s.close()
				return nil, fmt.Errorf("%s responded with failure status.", endpoint)
			case StatusSuccess:
				return ack.Payload, nil
			case StatusRedirect:
				log.Debug("Redirected to %s.", ack.Payload)
				s.HostPort = string(ack.Payload)
				attempt = 0
			default:
			}

		}

		s.close()
		s.backoff()

	}

	return nil, ABORT

}

// send makes a single attempt to dial the endpoint if it's closed. Then it
// sends the given request. If a request is not provided, it returns after
// dialing the websocket connection.
func (s *Socket) send(endpoint string, request interface{}) error {

	var err error
	if nil == s.Conn {
		log.Debug("Dialing %s.", endpoint)
		s.Conn, err = websocket.Dial(endpoint, "", s.Origin)
		if nil != err || nil == request {
			return err
		}
	}

	if err := websocket.JSON.Send(s.Conn, request); nil != err {
		s.close()
		return err
	}

	return nil

}

// Acknowledge makes a single attempt to send an acknowledgement.
func (s *Socket) Acknowledge(ack interface{}) error {

	s.lock.Lock()
	defer s.lock.Unlock()

	if nil == s.Conn {
		return io.EOF
	}

	return websocket.JSON.Send(s.Conn, ack)

}

// Receive waits on the associated websocket connection and unmarshals the
// desired. An error is returned is the connection is deemed unsable.
func (s *Socket) Receive(value interface{}) error {

	s.lock.Lock()
	defer s.lock.Unlock()
	return s.receive(value)

}

// receive waits on the connection for a single message. Caller must have
// socket lock before invoking this.
func (s *Socket) receive(value interface{}) error {

	if nil == s.Conn {
		return io.EOF
	}

	conn := s.Conn
	s.lock.Unlock()
	defer s.lock.Lock()

	for {

		err := websocket.JSON.Receive(conn, value)

		switch err {
		case nil:
			return nil
		default:
			e, ok := err.(net.Error)
			if ok && e.Temporary() {
				continue
			}
		}

		s.close()
		return err

	}

	return nil

}

// backoff sleeps for a random number of milliseconds that is less than the
// MAX_RETRY_INTERVAL. This must be invoked while the caller is holding on to
// the socket lock.
func (s *Socket) backoff() {

	s.lock.Unlock()
	defer s.lock.Lock()

	duration := time.Duration(rand.Intn(MAX_RETRY_INTERVAL))
	log.Debug("Backing off %d milliseconds.", duration)
	time.Sleep(duration * time.Millisecond)

}
