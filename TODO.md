## Protocol
- Switch away from JSON to msgpack to allow binary data to allow streaming
  image, videos, and other binary messages.

## Consumer
- Unit Tests
- Implement close
- Sequence guarantee and rewind
- Add back-off and reconnect
- Implement redirect
- Failures and Error Handling
- Allow WSS

## Producer
+ Publish messages
+ Sequence Numbers
- Unit Tests
- Send and wait for ack
- Retry and reconnection
- Implement redirect
- Failures and Error Handling

## Broker
- CRC32 checksum verification in broker
