## Protocol
- Switch away from JSON to msgpack to allow binary data to allow streaming
  image, videos, and other binary messages.

## Consumer
- Sequence guarantee and rewind
- Add back-off and reconnect
- Implement redirect
- Failures and error Handling
- Implement close
- Unit Tests

## Producer
+ Publish messages
+ Sequence Numbers
+ Failures and error Handling
+ Retry and reconnection
- Implement redirect
- Send and wait for ack
- Unit Tests

## Broker
- CRC32 checksum verification in broker
- Delete closed subscriptions
