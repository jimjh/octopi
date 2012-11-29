package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"io"
	"octopi/api/protocol"
	"octopi/util/log"
	"sync"
)

type Follower struct {
	conn  *websocket.Conn // open connection
	tails Offsets         // tails of log files
}

// SyncFollower streams updates to a follower through the given connection.
// Once the follower has fully caught it, add it to the follower set.
func (b *Broker) SyncFollower(conn *websocket.Conn, tails Offsets) {

	follower := &Follower{conn, tails}

	log.Debug("Begin synchronizing follower.")
	// for !follower.caughtUp(b) {
	// TODO: use cond var to determine fully sync'ed
	// TODO: return follower
	follower.catchUp(b)
	// }

}

// caughtUp checks if the follower has really caught up, and adds it to the
// broker's follower set.
func (f *Follower) caughtUp(broker *Broker) bool {
	return false
}

// catchUp tries to catch up the follower's log files with the leader's.
func (f *Follower) catchUp(broker *Broker) error {

	// TODO: what if follower dies while catching up?

	broker.lock.Lock()
	logs := broker.logs
	broker.lock.Unlock()

	var group sync.WaitGroup
	var abort error

	for topic, _ := range logs {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := f.catchUpLog(broker, topic); nil != err {
				abort = err
			}
		}()
	}

	group.Wait()
	return abort

}

// catchUpLog synchronizes a single log with the follower. Returns an error if
// it votes to abort the synchronization.
func (f *Follower) catchUpLog(broker *Broker, topic string) error {

	file, err := OpenLog(broker.config, topic, f.tails[topic])
	if nil != err {
		log.Warn("Could not open log file for topic: %s.", topic)
		return nil
	}

	defer file.Close()

	for {

		// read next entry
		entry, err := file.ReadNext()
		if nil != err {
			return nil
		}

		// send to follower
		sync := &protocol.Sync{topic, entry.RequestId, entry.Message}
		if err = websocket.JSON.Send(f.conn, sync); nil != err {
			return err
		}

	}

	return nil

}

// catchUp tries to bring _this_ broker up to date with its leader.
func (b *Broker) catchUp() error {

	// TODO: what if leader dies while follower is catching up?
	write := func(request *protocol.Sync) error {

		file, err := b.getOrOpenLog(request.Topic)
		if nil != err {
			return err
		}

		entry := &LogEntry{request.Message, request.RequestId}
		return file.WriteNext(entry)

	}

	for {

		var request protocol.Sync
		err := websocket.JSON.Receive(b.leader, &request)

		switch err {
		case nil:
			if err := write(&request); nil != err {
				log.Warn("Unable to open log file for %s.", request.Topic)
			}
		case io.EOF:
			log.Error("Connection with leader lost.")
			return err
		default:
			log.Error("Ignoring invalid message from leader: %s.", err.Error())
		}

	}

	return nil

}
