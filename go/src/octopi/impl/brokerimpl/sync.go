package brokerimpl

import (
	"code.google.com/p/go.net/websocket"
	"encoding/json"
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
// Once the follower has fully caught up, add it to the follower set.
func (b *Broker) SyncFollower(conn *websocket.Conn, tails Offsets, hostport protocol.HostPort) error {

	follower := &Follower{
		conn:     conn,
		tails:    tails,
		hostport: hostport,
		quit:     make(chan interface{}, 1),
	}

	if err := b.ackFollower(follower); nil != err {
		return err
	}

	log.Debug("Begin synchronizing follower %v.", follower.hostport)
	for !follower.caughtUp(b) {
		if err := follower.catchUp(b); nil != err {
			return err
		}
	}

	log.Info("Follower %v has fully caught up.", follower.hostport)

	<-follower.quit
	return nil

}

// ackFollower sends an acknowledgement to the follower.
func (b *Broker) ackFollower(f *Follower) error {

	b.lock.Lock()
	defer b.lock.Unlock()

	ack := new(protocol.Ack)
	if b.role != LEADER || f.conn.RemoteAddr().String() == b.Origin() {
		log.Warn("Denying follow requests from %s.", f.conn.RemoteAddr())
		ack.Status = protocol.StatusFailure
	} else {

		inner := new(protocol.FollowACK)
		inner.Truncate = make(Offsets)
		for topic, checkpoint := range b.checkpoints {
			if f.tails[topic] > checkpoint {
				inner.Truncate[topic] = checkpoint
				f.tails[topic] = checkpoint
			}
		}

		ack.Status = protocol.StatusSuccess
		ack.Payload, _ = json.Marshal(inner)

	}

	return websocket.JSON.Send(f.conn, ack)

}

// caughtUp checks if the follower has really caught up, and adds it to the
// broker's follower set.
func (f *Follower) caughtUp(broker *Broker) bool {

	broker.lock.Lock()
	defer broker.lock.Unlock()

	//log.Info("Obtained lock for %v", f.hostport)

	expected := broker.tails()
	for topic, offset := range expected {
		// FIXME
		// if offset < f.tails[topic] {
		//	log.Panic("offset: %d, tail: %d", offset, f.tails[topic])
		//}
		if offset != f.tails[topic] {
			//log.Debug("Not fully caught up yet for %s. %d -> %d", topic, f.tails[topic], offset)
			return false
		}
	}

	// check if follower already in set. if so, delete prev entry.
	for follower, _ := range broker.followers {
		if f.hostport==follower.hostport{
			delete(broker.followers, follower)
		}
	}

	// add to set of followers
	broker.followers[f] = true

	// create struct to communicate with register
	var addFollow protocol.InsyncChange
	addFollow.Type = protocol.ADD
	addFollow.HostPort = f.hostport

	// add in-sync follower
	// check if disconnect from register. if so, exit.
	websocket.JSON.Send(broker.regConn, addFollow)
	// checkError(err) // FIXME: exiting is not the correct thing to do

	return true

}

// catchUp tries to catch up the follower's log files with the leader's. If the
// follower dies while catching up, the sync will be aborted.
func (f *Follower) catchUp(broker *Broker) error {

	logs := make([]string, len(broker.logs))

	broker.lock.Lock()
	for topic, _ := range broker.logs {
		logs = append(logs, topic)
	}
	broker.lock.Unlock()

	for _, topic := range logs {
		if err := f.catchUpLog(broker, topic); nil != err {
			return err
		}
	}

	return nil

}

// catchUpLog synchronizes a single log with the follower. Returns an error if
// it votes to abort the synchronization.
func (f *Follower) catchUpLog(broker *Broker, topic string) error {

	file, err := OpenLog(broker.config, topic, f.tails[topic])
	if nil != err {
		log.Warn("Could not open log file for topic: %s.", topic)
		return err
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
		if err := b.leader.Receive(&request); nil != err {
			log.Warn("Unable to receive from leader.")
			return err
		}

		offset, err := write(&request)
		if nil != err {
			log.Warn("Unable to open log file for %s.", request.Topic)
			continue
		}

		ack := &protocol.SyncACK{request.Topic, offset}
		if err := b.leader.Acknowledge(ack); nil != err {
			log.Warn("Unable to ack leader: %s", err.Error())
		}

		b.cond.Broadcast()

	}

	return nil

}
