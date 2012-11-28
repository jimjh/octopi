package regimpl

import(
	"code.google.com/p/go.net/websocket"
	"octopi/api/protocol"
	"sync"
)

const(
	EMPTY = ""
)

type Register struct{
	leader protocol.URL
	insync InsyncSet
	lock sync.Mutex
}

// InsyncSet is an implementation of a set of hostports
type InsyncSet map[protocol.Hostport]bool

// NewRegister returns a new Register object
func NewRegister() *Register{
	reg := &Register{
			insync: make(InsyncSet),
		}

	return reg
}

// NoLeader returns whether or not the register has a leader or not
func (r *Register) NoLeader() bool{
	websocket.Dial("","","")
	return r.leader == EMPTY
}

// LeaderDisconnect empties out the leader and notifies followers
// of a change in leader
func (r *Register) LeaderDisconnect(){
	r.lock.Lock()

	// create a copy to release lock earlier
	tmpSet := make(InsyncSet)

	for hp, _ := range r.insync{
		tmpSet[hp] = true
	}

	r.leader = EMPTY
	r.lock.Unlock()

	for hp, _ := range tmpSet{
		go notifyFollower(hp)
	}
}

// notifyFollowers notifies the followers of a change in leader
func notifyFollower(follower protocol.Hostport){
}

// GetInsyncSet returns the list of in-sync followers
func (r *Register) GetInsyncSet() InsyncSet{
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.insync
}

// AddFollower adds a follower to the list of in-sync followers
func (r *Register) AddFollower(follower protocol.Hostport){
	r.lock.Lock()
	defer r.lock.Unlock()
	r.insync[follower] = true
}

// RemoveFollower removes a follower from the list of in-sync followers
func (r *Register) RemoveFollower(follower protocol.Hostport){
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.insync, follower)
}
