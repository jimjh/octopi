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
	if err := websocket.JSON.Receive(conn, &request); nil != err {
		log.Warn("Ignoring message from %v: %s", conn.RemoteAddr(), err.Error())
		return
	}

	log.Info("Received follow request from %v.", request.HostPort)

	if err := broker.SyncFollower(conn, request.Offsets, request.HostPort); nil != err {
		log.Error("Error sync'ing follower: %s", err.Error())
	}

}
