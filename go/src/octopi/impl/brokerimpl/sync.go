package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"encoding/json"
	"io"
	"net"
	"octopi/api/protocol"
	"octopi/util/log"
)

// The Follower struct contains the connection, reported tails of the
// follower's log files, and the host:port of the follower. The quit channel is
// used to instruct the follower to stop syncing.
type Follower struct {
	conn     *websocket.Conn   // open connection
	tails    Offsets           // tails of log files
	hostport protocol.HostPort // hostport of the follower
	quit     chan interface{}  // quit channel
}

// SyncFollower streams updates to a follower through the given connection.
// Once the follower has fully caught it, add it to the follower set.
func (b *Broker) SyncFollower(conn *websocket.Conn, tails Offsets, hostport protocol.HostPort) error {

	follower := &Follower{
		conn:     conn,
		tails:    tails,
		hostport: hostport,
		quit:     make(chan interface{}, 1),
	}

	b.lock.Lock()
	ack := new(protocol.FollowACK)
	if nil != b.checkpoints {
		ack.Truncate = make(Offsets)
		for topic, checkpoint := range b.checkpoints {
			if tails[topic] > checkpoint {
				ack.Truncate[topic] = checkpoint
				tails[topic] = checkpoint
			}
		}
	}

	inner, _ := json.Marshal(ack)
	websocket.JSON.Send(conn, &protocol.Ack{protocol.StatusSuccess, inner})
	b.lock.Unlock()

	log.Debug("Begin synchronizing follower.")
	for !follower.caughtUp(b) {
		if err := follower.catchUp(b); nil != err {
			return err
		}
	}

	log.Info("Follower has fully caught up.")

	<-follower.quit
	return nil

}

// caughtUp checks if the follower has really caught up, and adds it to the
// broker's follower set.
func (f *Follower) caughtUp(broker *Broker) bool {

	log.Info("Catching up with %v", f.hostport)
	broker.lock.Lock()
	defer broker.lock.Unlock()

	log.Info("Obtained lock for %v", f.hostport)

	expected := broker.tails()
	for topic, offset := range expected {
		if offset != f.tails[topic] {
			return false
		}
	}

	// add to set of followers
	broker.followers[f] = true
	for follower, _ := range broker.followers {
		log.Info("Followers include: %v", follower.hostport)
	}

	// create struct to communicate with register
	var addFollow protocol.InsyncChange
	addFollow.Type = protocol.ADD
	addFollow.HostPort = f.hostport

	// add in-sync follower
	// check if disconnect from register. if so, exit.
	err := websocket.JSON.Send(broker.regConn, addFollow)
	checkError(err)

	return true

}

// catchUp tries to catch up the follower's log files with the leader's.
func (f *Follower) catchUp(broker *Broker) error {

	// TODO: what if follower dies while catching up?

	broker.lock.Lock()
	logs := broker.logs
	broker.lock.Unlock()

	var abort error

	for topic, _ := range logs {
		if err := f.catchUpLog(broker, topic); nil != err {
			abort = err
		}
	}

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
		sync := &protocol.Sync{topic, entry.Message, entry.RequestId}
		log.Debug("Wrote %v on topic %v to %v", entry.Message.ID, topic, f.hostport)
		if err = websocket.JSON.Send(f.conn, sync); nil != err {
			return err
		}

		// wait for ack
		var ack protocol.SyncACK
		if err = websocket.JSON.Receive(f.conn, &ack); nil != err {
			return err
		}

		f.tails[topic] = ack.Offset

	}

	return nil

}

// catchUp tries to bring _this_ broker up to date with its leader.
func (b *Broker) catchUp() error {

	// TODO: what if leader dies while follower is catching up?
	// FIXME: check for dups in the log

	write := func(request *protocol.Sync) (int64, error) {

		b.lock.Lock()
		defer b.lock.Unlock()

		file, err := b.getOrOpenLog(request.Topic)
		if nil != err {
			return 0, err
		}

		entry := &LogEntry{request.Message, request.RequestId}
		if err = file.WriteNext(entry); nil != err {
			return 0, err
		}

		stat, err := file.Stat()
		if nil != err {
			return 0, err
		}

		return stat.Size(), nil

	}

	for {

		var request protocol.Sync
		err := b.leader.Receive(&request)
		log.Debug("Received %v.", request)

		switch err {
		case nil:
			offset, err := write(&request)
			if nil != err {
				log.Warn("Unable to open log file for %s.", request.Topic)
			}
			ack := &protocol.SyncACK{request.Topic, offset}
			if nil != websocket.JSON.Send(b.leader.Conn, ack) {
				log.Warn("Unable to ack leader.")
			}
			b.cond.Broadcast()
		case io.EOF:
			log.Error("Connection with leader lost.")
			return err
		default:
			e, ok := err.(net.Error)
			if ok && !e.Temporary() {
				return err
			}
			log.Error("Ignoring invalid message from leader:", err)
		}

	}

	return nil

}
