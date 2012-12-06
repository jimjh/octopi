Octopi Documentation

# Architecture

## Components of the system

1. Register: The central housekeeping piece of the system. Maintains membership information and keeps an open connection to the leader to detect leader failure. Also in charge of redirecting other components to the leader.
2. Producer: Generates and sends content under a designated topic to the leader broker.
3. Broker: Can either be a leader or a follower. There can only be one leader at a time. Relays the messages from producers to consumers. All brokers keep a log of messages they have received, and the leader broker maintains connections to all following brokers.
4. Consumer: Consumes messages from brokers. Can specify offset to obtain messages only after that particular offset in the logs of brokers.

## System Flow

At system startup, we designate one and only one broker as the leader and however many other brokers as followers of the leader. The leader contacts the register and becomes the leader. To follow the leader, all the other brokers first contact the register in order to obtain the address of the leader. The brokers then contact the leader, issuing a request to follow it.

Producers send messages to brokers, which maintains a log for each topic. Messages from the topic logs are streamed to consumers, and a consumer may specify an optional starting offset during subscription.

Primary-backup replication is used in Octopi to tolerate broker failures. Followers are replicas of the leader broker, and they try to keep in sync with the leader's log.

Consumers, in order to stream from brokers, must contact the register first. The register will then redirect the consumer to the leader of the topic, which will in turn redirect the consumer again to a broker (could be the leader) that is in-sync with the leader. The consumer then receives its stream from that particular broker.

On leader disconnections, a leader transition, discussed in more detail below, occurs and we elect a new leader. On follower disconnection, the producer/consumer simply contacts the register again to find a new broker.

#Implementation Details

## Produce Requests
New messages received from producers are replicated across all followers before acknowledgements are sent. The process is as follows:

1. Producer discovers identity of leader through register
2. Producer publishes new message to leader
3. Leader appends message to log file
4. Followers pull message off log file, append to their log files, and acknowledge leader
5. Leader waits for all acknowledgements before replying producer

Note:

* Producer should timeout and retry if acknowledgement is not received
* Each produce request includes a sequence number that is used to detect duplicate produce requests from the same producer
* Leader must detect lost followers and delete them from the set

### Failure Conditions

The following failure possibilities are taken into consideration:

1. leader fails before writing to its log
	- producer should timeout and retry
2. leader fails after writing to its log, but before replying producer
	- producer should timeout and retry
	- if new leader did not receive the message, the retry will succeed
	- else, the duplicate will be detected using the sequence numbers
3.  leader fails after replying producer
	- producer will not retry, but all is well

## Leadership Transition

The register is responsible for managing broker membership and only maintains one connection, which is the connection to the leader. Leader transition occurs when the connection between the leader and the broker is lost

During leader transition, the following occurs:

1. Register announces leadership change and actively contacts (but does not maintain the contact) to in-sync followers
2. The followers stop listening to new leader
3. The register sends a list of all in-sync followers, and the followers decide by using the highest CRC32 hash to determine who is the new leader. The followers reach the consensus on its own.
4. The follower then knows if it is the leader or not. If the follower turns out to be the leader, it automatically contacts the register to become the leader. If not, it contacts the register to wait for a redirect (which only occurs after the new leader contacts the register to become the leader)
5. If the register does not obtain any requests to become the leader, it tries to contact all in-sync followers with a list again after a designated amount of time

Note:

- The register sends the same list to everyone. The list is the list of in-sync followers at the time of leader transition
- The register removes followers from the list when it cannot contact the follower. If it notices that a follower has dropped after it has already contacted a few other followers, it will not remove the follower from the list yet. It will simply send the same list, and will only remove the follower from the list on the next round of contact (if no leader contacts it)

**Edge Case**: should the network be partitioned such that the leader (and a few followers) are cut off from the register and the rest of the brokers, there is a potential that two contending groups will form. However, since producers/consumers always discover the leader via the register, the register's decision wins.

#Assumptions

## Websocket
- The Go implementation of websocket is correct, though the websocket package is not in its official list of packages
- Websocket is based on TCP and automatically re-tries and detects disconnections

## Security
- No malicious listeners to spy on content
- No entity to disrupt traffic
- No entity to partition network

## Performance
- Sequential writes are fast
- "Acceptable" network connection between broker nodes to allow streaming

## Failures
- Ability to survive and maintain data in the scenario of less than n-1 broker failures, where n is the total amount of brokers
- Always more than one in-sync follower at time of failure