package main

import (
	"code.google.com/p/go.net/websocket"
	"octopi/api/protocol"
	"octopi/util/log"
)

// follower handles incoming follow requests.
func follower(conn *websocket.Conn) {

	defer conn.Close()

	var request protocol.FollowRequest
	if nil != websocket.JSON.Receive(conn, &request) {
		log.Warn("Ignoring invalid message from %v.", conn.RemoteAddr())
		return
	}

	log.Info("Received follow request from %v.", conn.RemoteAddr())

	// conn := &protocol.Follower{conn}
	// TODO: broker.RegisterFollower(conn, request.Offsets)
	// err = websocket.JSON.Send(conn, protocol.FollowACK{})

	// deal with sync
	/*for {
		var ack protocol.SyncACK
		err := websocket.JSON.Receive(ws, &ack)
		if err == io.EOF {
			break
		}
		broker.SyncFollower(conn, ack)
	}

	broker.DeleteFollower(conn)*/

}
