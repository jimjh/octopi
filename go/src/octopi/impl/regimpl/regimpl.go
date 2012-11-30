package regimpl

import (
	"code.google.com/p/go.net/websocket"
	"octopi/api/protocol"
	"sync"
	"time"
)

const (
	EMPTY = ""
)

const (
	// time in ms to allow new leader to contact register
	// before sending new leader requests
	LEADERWAIT = 10000
)

type Register struct {
	leader string
	insync map[string]bool
	lock   sync.Mutex
}

// NewRegister returns a new Register object
func NewRegister() *Register {
	reg := &Register{
		insync: make(map[string]bool),
	}

	return reg
}

func (r *Register) Leader() string {
	return r.leader
}

// NoLeader returns whether or not the register has a leader or not
func (r *Register) NoLeader() bool {
	return r.leader == EMPTY
}

func (r *Register) SetLeader(hostport string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.leader = hostport
}

// LeaderDisconnect empties out the leader and notifies followers
// of a change in leader
func (r *Register) LeaderDisconnect() {
	r.lock.Lock()

	// create a copy to release lock earlier
	tmpSet := make(map[string]bool)

	for hp, _ := range r.insync {
		tmpSet[hp] = true
	}

	r.lock.Unlock()

	// notify all followers with the same set for consistency 
	// and only remove from original set if fail to contact
	for hp, _ := range tmpSet {
		go r.notifyFollower(hp, tmpSet)
	}
}

// CheckNewLeader allows re-sending of disconnect requests
// every interval until a leader is connected 
func (r *Register) CheckNewLeader() {
	for r.leader == EMPTY {
		time.Sleep(LEADERWAIT * time.Millisecond)
		r.LeaderDisconnect()
	}
}

// notifyFollowers notifies the followers of a change in leader
func (r *Register) notifyFollower(follower string, is map[string]bool) {
	conn, err := websocket.Dial("ws://"+follower+"/"+protocol.REGISTER, "", "http://"+follower+"/")

	// failed to contact the follower
	if nil != err {
		// remove from follower set
		r.RemoveFollower(follower)
		return
	}

	err = websocket.JSON.Send(conn, is)

	// failed to send to the follower
	if nil != err {
		// remove from follower set
		r.RemoveFollower(follower)
		return
	}
}

// GetInsyncSet returns the list of in-sync followers
func (r *Register) GetInsyncSet() map[string]bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.insync
}

// AddFollower adds a follower to the list of in-sync followers
func (r *Register) AddFollower(follower string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.insync[follower] = true
}

// RemoveFollower removes a follower from the list of in-sync followers
func (r *Register) RemoveFollower(follower string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.insync, follower)
}
