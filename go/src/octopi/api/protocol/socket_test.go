package protocol

import (
	"code.google.com/p/go.net/websocket"
	"encoding/binary"
	"math"
	"net"
	"net/http"
	"octopi/util/test"
	"testing"
	"time"
)

const fakeOrigin = "localhost:12345"

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
	}
	attempts := 5
	_, err = socket.Send(nil, attempts, fakeOrigin)
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
		*requestCount++
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
	count1 := 0
	count2 := 0

	listener1, err := net.Listen("tcp", ":11111")
	t.AssertNil(err, "net.Listen")

	listener2, err := net.Listen("tcp", ":11112")
	t.AssertNil(err, "net.Listen")

	server1 := &http.Server{
		Handler: websocket.Handler(redirect(&count1)),
	}

	server2 := &http.Server{
		Handler: websocket.Handler(accept(&count2)),
	}

	go server1.Serve(listener1)
	go server2.Serve(listener2)

	socket := &Socket{
		HostPort: "localhost:11111",
		Path:     "",
		Origin:   "localhost:12345",
	}
	_, err = socket.Send(nil, 3, fakeOrigin)
	t.AssertNil(err, "socket.Send")

	listener1.Close()
	listener2.Close()

	matcher := new(test.IntMatcher)
	t.AssertEqual(matcher, 1, count1)
	t.AssertEqual(matcher, 1, count2)

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
	}
	_, err = socket.Send(nil, 3, fakeOrigin)
	t.AssertNotNil(err, "socket.Send")

	listener.Close()
	t.AssertEqual(new(test.IntMatcher), 1, requestCount)

}

// TestResetSend ensures that Sends are interrupted.
func TestResetSend(tester *testing.T) {

	t := test.New(tester)
	count1 := 0
	count2 := 0

	listener1, err := net.Listen("tcp", ":11111")
	t.AssertNil(err, "net.Listen")

	listener2, err := net.Listen("tcp", ":11112")
	t.AssertNil(err, "net.Listen")

	server1 := &http.Server{Handler: websocket.Handler(count(&count1))}

	server2 := &http.Server{Handler: websocket.Handler(accept(&count2))}

	go server1.Serve(listener1)
	go server2.Serve(listener2)

	socket := &Socket{
		HostPort: "localhost:11111",
		Path:     "",
		Origin:   fakeOrigin,
	}

	go socket.Send(nil, math.MaxInt32, fakeOrigin)
	time.Sleep(1 * time.Second)

	socket.Reset("localhost:11112")
	time.Sleep(1 * time.Second)

	listener1.Close()
	listener2.Close()

	t.AssertPositive(int64(count1), "socket.Send")
	t.AssertEqual(new(test.IntMatcher), 1, count2)

}

// TestResetReceive ensures that Receives are interrupted.
func TestResetReceive(tester *testing.T) {

	t := test.New(tester)
	count := 0

	listener, err := net.Listen("tcp", ":11111")
	t.AssertNil(err, "net.Listen")

	server := &http.Server{Handler: websocket.Handler(accept(&count))}
	go server.Serve(listener)

	socket := &Socket{
		HostPort: "localhost:11111",
		Path:     "",
		Origin:   fakeOrigin,
	}

	request := new(Sync)
	_, err = socket.Send(request, 5, fakeOrigin)
	t.AssertNil(err, "socket.Send")

	go func() {
		time.Sleep(1 * time.Second)
		socket.Reset("localhost:11112")
	}()

	err = socket.Receive(new(Ack))
	t.AssertNotNil(err, "socket.Receive")

	listener.Close()

}

func numbers(conn *websocket.Conn) {

	for i := 0; i <= 10; i++ {
		ack := &Ack{Status: StatusSuccess, Payload: make([]byte, 10)}
		binary.PutVarint(ack.Payload, int64(i))
		websocket.JSON.Send(conn, ack)
	}

}

// TestReceive ensures that the socket can read from the connection.
func TestReceive(tester *testing.T) {

	t := test.New(tester)

	listener, err := net.Listen("tcp", ":11111")
	t.AssertNil(err, "net.Listen")

	server := &http.Server{Handler: websocket.Handler(numbers)}
	go server.Serve(listener)

	socket := &Socket{
		HostPort: "localhost:11111",
		Path:     "",
		Origin:   fakeOrigin,
	}

	request := new(Sync)
	_, err = socket.Send(request, 5, fakeOrigin)
	t.AssertNil(err, "socket.Send")

	for i := 1; i <= 10; i++ {
		ack := new(Ack)
		err = socket.Receive(ack)
		t.AssertNil(err, "socket.Receive")
		val, _ := binary.Varint(ack.Payload)
		t.AssertEqual(new(test.IntMatcher), i, int(val))
	}

	listener.Close()

}
