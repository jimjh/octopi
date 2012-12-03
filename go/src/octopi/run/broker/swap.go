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

	var max uint32 = 0
	var maxhp string

	// deterministically determine the leader using the highest crc32 hash
	for hp, _ := range insyncSet {
		cksm := crc32.ChecksumIEEE([]byte(hp))
		if cksm > max {
			maxhp = hp
			max = cksm
		}
	}

	if maxhp == broker.Origin() {
		log.Debug("I should become the new leader. I am %v", broker.Origin())
		broker.BecomeLeader()
		log.Debug("I am the new leader.")
	} else {
		broker.ChangeLeader()
	}

}
