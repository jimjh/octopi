package protocol

import (
	"code.google.com/p/go.net/websocket"
	"net"
	"net/http"
	"octopi/util/test"
	"testing"
	"time"
)

func count(requestCount *int) func(*websocket.Conn) {
	return func(conn *websocket.Conn) {
		*requestCount++
		conn.Close()
	}
}

// TestRetries ensures that the socket retries a few times before giving up.
func TestRetries(tester *testing.T) {

	t := test.New(tester)
	requestCount := 0

	listener, err := net.Listen("tcp", ":11111")
	t.AssertNil(err, "net.Listen")

	server := &http.Server{
		Handler:      websocket.Handler(count(&requestCount)),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go server.Serve(listener)

	socket := &Socket{
		HostPort: "localhost:11111",
		Path:     "",
		Origin:   "localhost:12345",
		Msg:      Ack{},
	}
	attempts := 5
	_, err = socket.Send(nil, attempts)
	t.AssertNotNil(err, "socket.Send")

	listener.Close()
	matcher := new(test.IntMatcher)
	t.AssertEqual(matcher, attempts, requestCount)

}

func redirect(requestCount *int) func(*websocket.Conn) {
	return func(conn *websocket.Conn) {
		ack := &Ack{StatusRedirect, []byte("localhost:11112")}
		websocket.JSON.Send(conn, ack)
		*requestCount++
	}
}

func accept(requestCount *int) func(*websocket.Conn) {
	return func(conn *websocket.Conn) {
		ack := &Ack{StatusSuccess, make([]byte, 0)}
		websocket.JSON.Send(conn, ack)
		for {
			var request interface{}
			websocket.JSON.Receive(conn, request)
		}
	}
}

// TestRedirects ensures that the socket follows redirects, if they are given.
func TestRedirects(tester *testing.T) {

	t := test.New(tester)
	requestCount := 0

	listener1, err := net.Listen("tcp", ":11111")
	t.AssertNil(err, "net.Listen")

	listener2, err := net.Listen("tcp", ":11112")
	t.AssertNil(err, "net.Listen")

	server1 := &http.Server{
		Handler: websocket.Handler(redirect(&requestCount)),
	}

	server2 := &http.Server{
		Handler: websocket.Handler(accept(&requestCount)),
	}

	go server1.Serve(listener1)
	go server2.Serve(listener2)

	socket := &Socket{
		HostPort: "localhost:11111",
		Path:     "",
		Origin:   "localhost:12345",
		Msg:      new(Ack),
	}
	_, err = socket.Send(nil, 3)
	t.AssertNil(err, "socket.Send")

	listener1.Close()
	listener2.Close()

	matcher := new(test.IntMatcher)
	t.AssertEqual(matcher, 1, requestCount)

}

func fail(requestCount *int) func(*websocket.Conn) {
	return func(conn *websocket.Conn) {
		ack := &Ack{StatusFailure, nil}
		websocket.JSON.Send(conn, ack)
		*requestCount += 1
	}
}

// TestFailure ensures that Send returns an error on failure.
func TestFailure(tester *testing.T) {

	t := test.New(tester)
	requestCount := 0

	listener, err := net.Listen("tcp", ":11111")
	t.AssertNil(err, "net.Listen")

	server := &http.Server{
		Handler: websocket.Handler(fail(&requestCount)),
	}

	go server.Serve(listener)

	socket := &Socket{
		HostPort: "localhost:11111",
		Path:     "",
		Origin:   "localhost:12345",
		Msg:      Ack{},
	}
	_, err = socket.Send(nil, 3)
	t.AssertNotNil(err, "socket.Send")

	listener.Close()

	matcher := new(test.IntMatcher)
	t.AssertEqual(matcher, 1, requestCount)

}

func stream(requestCount *int) func(*websocket.Conn) {
	return func(conn *websocket.Conn) {
		ack := &Ack{StatusSuccess, nil}
		websocket.JSON.Send(conn, ack)
		*requestCount++
		for i := 0; i < 10; i++ {
			ack := &SyncACK{"", int64(i)}
			websocket.JSON.Send(conn, ack)
		}
	}
}

func TestReceive(tester *testing.T) {

	t := test.New(tester)
	requestCount := 0

	listener, err := net.Listen("tcp", ":11111")
	t.AssertNil(err, "net.Listen")

	server := &http.Server{
		Handler: websocket.Handler(stream(&requestCount)),
	}

	go server.Serve(listener)

	socket := &Socket{
		HostPort: "localhost:11111",
		Path:     "",
		Origin:   "localhost:12345",
		Msg:      SyncACK{},
	}
	_, err = socket.Send(nil, 3)
	t.AssertNil(err, "socket.Send")

	listener.Close()

	matcher := new(test.IntMatcher)

	var expected int64 = 0
	for expected = 0; expected < 10; expected++ {
		var msg SyncACK
		err = socket.Receive(&msg)
		t.AssertNil(err, "socket.Receive")
		if expected != msg.Offset {
			tester.Errorf("Expected %d, was %d.", expected, msg.Offset)
		}
	}

	t.AssertEqual(matcher, 1, requestCount)

}
