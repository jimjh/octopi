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
- CRC32 checksum verification in broker.
- Unit Tests
- Send and wait for ack.
- Sequence Numbers.
- Retry and reconnection
- Implement redirect
- Failures and Error Handling
