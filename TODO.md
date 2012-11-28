## Protocol
- Switch away from JSON to msgpack to allow binary data to allow streaming
  image, videos, and other binary messages.
+ Configuration file

## Consumer
+ Implement close
+ Failures and error handling
+ Add back-off and reconnect
+ Subscribe and wait for ACK
+ Send offsets
+ Consume from offsets (file)
+ Consume from offsets (producer)
- Fix panics
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
- CRC32 checksum verification in broker
- Robust logging and recovery
- Leader and follower
- Switch to cond vars for Produce method to wait for enough FollowerACKs
- Ack producer requests
- Implement redirects
- Implement timer flush

## Register

- documentation
- demo app
