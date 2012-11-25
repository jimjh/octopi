## Protocol
- Switch away from JSON to msgpack to allow binary data to allow streaming
  image, videos, and other binary messages.
- Configuration file

## Consumer
+ Implement close
+ Failures and error handling
+ Add back-off and reconnect
- Sequence guarantee and rewind
- Subscribe and wait for ACK
- Unit Tests
- Implement redirect

## Producer
+ Publish messages
+ Sequence Numbers
+ Failures and error handling
+ Retry and reconnection
- Send and wait for ack
- Unit Tests
- Implement redirect

## Broker
+ Delete closed subscriptions
- CRC32 checksum verification in broker
- Robust logging and recovery
- Ack producer requests
- Ack consumer requests
- Leader and follower
- Implement redirects
