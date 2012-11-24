## Protocol
- Switch away from JSON to msgpack to allow binary data to allow streaming
  image, videos, and other binary messages.

## Consumer
+ Implement close
- Failures and error handling
- Add back-off and reconnect
- Sequence guarantee and rewind
- Subscribe and wait for ACK
- Implement redirect
- Unit Tests

## Producer
+ Publish messages
+ Sequence Numbers
+ Failures and error handling
+ Retry and reconnection
- Implement redirect
- Send and wait for ack
- Unit Tests

## Broker
- CRC32 checksum verification in broker
- Delete closed subscriptions
