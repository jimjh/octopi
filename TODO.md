## Register
- actual redirecting

## Consumer
- Restart from offsets (file)
- Receive special characters
- Fix unsubscribe
- Unit Tests
- Implement redirect

## Producer
- Unit Tests
- Implement redirect

## Broker
- Leader and follower
  - what if leader dies while follower is catching up?
- CRC32 checksum verification in broker
- Ignore duplicates
- tell register to remove dead followers
- Ignore produce requests if not leader
- Robust logging and recovery
- Switch to cond vars for Produce method to wait for enough FollowerACKs
- Ack producer requests
- Implement redirects
- Implement timer flush

## Net
- write generic function in go that opens connections and follows redirects
- write generic function that deals with network failures

- documentation
- demo app
