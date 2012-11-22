## Protocol
- Switch away from JSON to msgpack to allow binary data to allow streaming
  image, videos, and other binary messages.

## Consumer
- CRC32 verification
- Unit Tests
- Sequence guarantee and rewind
- Add back-off and reconnect
- Implement redirect
- Allow WSS
