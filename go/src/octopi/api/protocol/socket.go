package protocol

import (
	"code.google.com/p/go.net/websocket"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"octopi/util/log"
	"reflect"
	"time"
)

// Max number of milliseconds between retries.
const MAX_RETRY_INTERVAL = 2000

// ABORT is the error returned by Open if the maximum number of attempts has
// been exceeded.
var ABORT = errors.New("Exceeded maximum number of attempts.")

// Websocket protocol prefix
const ws = "ws://"

type Socket struct {
	hostport string          // host:port of target node
	path     string          // target url that is serving ws requests
	origin   string          // source origin (See websockets spec)
	msg      interface{}     // type of message that will be received
	Conn     *websocket.Conn // websocket connection
}

// Open sends the request to the given endpoint, and keeps trying until it
// succeeds or exceeds the maximum number of retries. If it encounters a
// redirect, the enclosed hostport is used as to find the new endpoint. Returns
// a channel that can be used to receive messages if there are no errors.
func (s *Socket) Open(request interface{}, attempts int) (<-chan interface{}, []byte, error) {

	endpoint := ws + s.hostport + "/" + s.path

	for attempt := 0; attempt < attempts; attempt++ {

		// open connection
		err := s.open(endpoint, request)
		if nil != err {
			log.Warn("Unable to open connection with %s: %s", err.Error())
			backoff()
			continue
		}

		// wait for acknowledgement
		var ack Ack
		err = websocket.JSON.Receive(s.Conn, &ack)
		if nil == err {

			// interpret status
			switch ack.Status {
			case StatusFailure:
				s.Conn.Close()
				return nil, nil, fmt.Errorf("%s responded with failure status.", endpoint)
			case StatusSuccess:
				channel := s.receive(&ack)
				return channel, ack.Payload, nil
			case StatusRedirect:
				log.Debug("Redirected to %s.", ack.Payload)
				endpoint = ws + string(ack.Payload) + "/" + s.path
				attempt = 0
			default:
			}

		}

		s.Conn.Close()
		backoff()

	}

	return nil, nil, ABORT

}

// open makes a single attempt to dial the endpoint and send the given request.
func (s *Socket) open(endpoint string, request interface{}) error {

	var err error
	s.Conn, err = websocket.Dial(endpoint, "", s.origin)
	if nil != err {
		return err
	}

	if nil != request {
		err = websocket.JSON.Send(s.Conn, request)
		if nil != err {
			s.Conn.Close()
			return err
		}
	}

	return nil

}

// receive spawns a goroutine that waits on messages from the given connection.
// The returned channel may be used to receive messages on this connection; the
// channel is closed when the connection is deemed usable.
func (s *Socket) receive(ack *Ack) <-chan interface{} {

	channel := make(chan interface{})

	go func() {

		msgType := reflect.TypeOf(s.msg)
		for {

			value := reflect.New(msgType)
			err := websocket.JSON.Receive(s.Conn, value.Interface())

			switch err {
			case nil:
				channel <- value.Interface()
			case io.EOF:
				close(channel)
				return
			default:
				e, ok := err.(net.Error)
				if ok && !e.Temporary() {
					close(channel)
					return
				}
				log.Error("Ignoring invalid message: %s", err.Error())
			}

		}

	}()

	return channel

}

// backoff sleeps for a random number of milliseconds that is less than the
// MAX_RETRY_INTERVAL.
func backoff() {
	duration := time.Duration(rand.Intn(MAX_RETRY_INTERVAL))
	log.Debug("Backing off %d milliseconds.", duration)
	time.Sleep(duration * time.Millisecond)
}
