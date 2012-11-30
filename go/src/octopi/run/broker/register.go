package main

import (
	"code.google.com/p/go.net/websocket"
	"hash/crc32"
	"octopi/util/log"
)

func register(ws *websocket.Conn) {

	defer ws.Close()

	log.Info("Received a leader change request from register")

	var insyncSet map[string]bool
	err := websocket.JSON.Receive(ws, &insyncSet)

	// return if receive invalid message or if connection breaks
	if nil != err || len(insyncSet) <= 0 {
		log.Warn("Ignoring invalid list from register")
		return
	}

	// close connection to previous leader
	broker.LeaderClose()

	var max uint32 = 0
	var maxhp string

	// deterministically determine the leader using the lowest crc32 hash
	for hp, _ := range insyncSet {
		cksm := crc32.ChecksumIEEE([]byte(hp))
		if cksm > max {
			maxhp = hp
			max = cksm
		}
	}

	if maxhp == broker.MyHostport() {
		log.Info("I have become the new leader. My hostport is: %v", broker.MyHostport())
		broker.BecomeLeader()
	} else {
		broker.LeaderChange(maxhp)
	}

}
