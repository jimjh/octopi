## Consumer
- Implement redirect
- Unit Tests
- Receive special characters

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
- check ACK sequence numbers

- documentation
- demo app
