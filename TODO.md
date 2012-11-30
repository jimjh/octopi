## Consumer
+ Implement close
+ Failures and error handling
+ Add back-off and reconnect
+ Subscribe and wait for ACK
+ Send offsets
+ Consume from offsets (file)
+ Consume from offsets (producer)
+ Fix panics
- Restart from offsets (file)
- Receive special characters
- Fix unsubscribe
- Unit Tests
- Implement redirect

## Producer
+ Publish messages
+ Sequence Numbers
+ Failures and error handling
+ Retry and reconnection
+ Send and wait for ack
- Unit Tests
- Implement redirect

## Broker
+ Delete closed subscriptions
+ Ack consumer requests
+ Open Log files
- Leader and follower
  - what if leader dies while follower is catching up?
- CRC32 checksum verification in broker
- Ignore duplicates
- Ignore produce requests if not leader
- Robust logging and recovery
- Switch to cond vars for Produce method to wait for enough FollowerACKs
- Ack producer requests
- Implement redirects
- Implement timer flush

- write generic function in go that opens connections and follows redirects

- documentation
- demo app
