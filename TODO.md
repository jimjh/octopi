## Consumer
- Restart from offsets (file)
- Receive special characters
- Unit Tests
- Implement redirect

## Producer
- Unit Tests

## Broker
- test deny self follow
- CRC32 checksum verification in broker
- Ignore duplicates
- Ignore produce requests if not leader
- Switch to cond vars for Produce method to wait for enough FollowerACKs
- Implement timer flush

## Net
+ write generic function in go that opens connections and follows redirects
+ write generic function that deals with network failures
- check ACK sequence numbers

- documentation
- demo app
