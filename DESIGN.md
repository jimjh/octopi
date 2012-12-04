# Architecture

_TODO: transfer stuff from proposal over here_
_TODO: include assumptions about go closing the connection _

Producers send messages to brokers, which maintains a log for each topic. Messages from the topic logs are streamed to consumers, and a consumer may specify an optional starting offset during subscription.

Primary-backup replication is used in Octopi to tolerate broker failures. Each partition contains a single leader with multiple followers. Followers are replicas of the leader, and they try to keep in sync with the leader's log.

## Produce Requests
New messages received from producers are replicated across all followers before acknowledgements are sent. The process is as follows:

1. producer discovers identity of leader through register
2. producer publishes new message to leader
3. leader appends message to log file
4. followers pull message off log file, append to their log files, and acknowledge leader
5. leader waits for all acknowledgements before replying producer

Note:

* producer should timeout and retry if acknowledgement is not received
* each produce request includes a sequence number that is used to detect duplicate produce requets from the same producer
* leader must detect lost followers and delete them from the set

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
The register is responsible to managing overall broker membership and leadership appointments. It maintains open connections with all brokers, and will try to reestablish these (a few times) if they are broken.

If the register decides to change the leader:

- register announces leadership change
- brokers stop listening to new leader, and accepts/rejects
- if register receives accepts from all _surviving brokers_, it
	- sends commit to all brokers
	- brokers receive commit and starts registering with new leader
- else, register sends abort to all brokers
	- brokers receive abort and resume operation with old leader

Note:

- if a broker fails to accept/reject after a few attempts, it will be dropped from the register's pool (considered dead.)
- when brokers send accepts to the register, it also contains the largest offsets for each topic that they have
	- register must tell new leader
	- new leader uses this to catchup with every broker and produce the latest logs possible

**Edge Case**: should the network be partitioned such that the leader (and a few followers) are cut off from the register and the rest of the brokers, there is a potential that two contending groups will form. However, since producers/consumers always discover the leader via the register, the register's decision wins.
