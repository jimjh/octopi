package messageapi

import(
	"container/list"
)

/* constants for contacting register server */
const(
	BROKER = iota
	PRODUCER
	CONSUMER
)

/* constants for register to tell which role a broker is taking */
const(
	LEADER = iota
	FOLLOWER
)

/* used by brokers to contact register */
type BrokerRegInit struct{
	MessageSrc int
	HostPort string
}

/* used by register to assign brokers */
type RegBrokerInit struct{
	Role int
	LeadUrl string //url of the leader, for followers
	LeadOrigin string //origin of the leader, for followers
	Topics *list.List //topics assigned by the register server 
}

/* used by producers to contact register */
type ProdRegInit struct{
	MessageSrc int
	Topic string
}

/* used by register to tell producer the lead broker */
type RegProdInit struct{
	LeadUrl string
	LeadOrigin string
}

/* used by consumers to contact register */
type ConsRegInit struct{
	MessageSrc int
	Topic string
}

/* used by register to tell consumer its assigned broker */
type RegConsInit struct{
	BrokerUrl string
	BrokerOrigin string
}

/* used by followers to contact leader */
type FollowLeadInit struct{
	MessageSrc int
	HostPort string
	//other fields to identify position of log
}
