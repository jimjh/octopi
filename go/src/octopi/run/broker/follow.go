package main

import (
	"code.google.com/p/go.net/websocket"
	"octopi/api/protocol"
	"octopi/util/log"
)

// follower handles incoming follow requests. Followers inform leader of the
// sizes (or offsets) of their log files, and the leader will stream updates to
// them. Followers that have fully caught up will be added to the leader's
// follower set.
func follower(conn *websocket.Conn) {

	defer conn.Close()
	defer log.Info("Connection with follower closed.")

	var request protocol.FollowRequest
	if nil != websocket.JSON.Receive(conn, &request) {
		log.Warn("Ignoring invalid message from %v.", conn.RemoteAddr())
		return
	}

	log.Info("Received follow request from %v.", conn.RemoteAddr())

	// TODO: ACK follow request
	// err = websocket.JSON.Send(conn, protocol.FollowACK{})

	if err := broker.SyncFollower(conn, request.Offsets, request.Hostport); nil != err {
		log.Error("Error sync'ing follower: %s", err.Error())
	}

}
