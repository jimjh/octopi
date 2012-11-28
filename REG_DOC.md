#Register responsibilities
1. Keeping member list of in-sync followers
2. Provide member list to consumers
3. Provide leader to producers
4. Provide leader to new brokers
5. Provide member list to all in-sync followers when leader dies and lets the 
individual followers deterministically find out who is leader

#Register handlers
1. leaderHandler: used in maintaining connection with leader and receiving status on in-sync followers
2. followerHandler: used in telling new joining brokers who the current leader is and redirects it
3. producerHandler: used in redirecting producer to current leader
4. consumerHandler: used in providing member list to consumers

#Register state-keeping
1. Set of in-sync followers (hostport), as communicated with the leader
2. Connection with the leader
3. URL identity of the leader

#On initial register startup
1. Set leader connection to nil
2. Wait for leader connection and set leader status
3. From now on, reject connections to leaderHandler if leader status is not nil (i.e. when leader is still alive)
4. Followers contact register via consumerHandler and register redirects them to the leader

#On leader failure:
1. Set leader to nil
2. Contacts all in-sync followers through websocket.Dial (if failed contact, remove from member list)
3. Provide set of in-sync followers to the set of in-sync followers
4. Set leader to nil and so leaderHandler will allow new connections
5. If wait a while and no leader contact, contact all in-sync followers through websocket.Dial again
6. See step 3
