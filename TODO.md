## Protocol
- Switch away from JSON to msgpack to allow binary data to allow streaming
  image, videos, and other binary messages.
+ Configuration file

## Consumer
+ Implement close
+ Failures and error handling
+ Add back-off and reconnect
+ Subscribe and wait for ACK
- Send offsets
- Consume from offsets
- Restart from offsets
- Receive special characters
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
- Switch to cond vars for Produce method to wait for enough FollowerACKs
- Open Log files
- Ack producer requests
- Ack consumer requests
- Leader and follower
- Implement redirects

##JSON Config
- Directory for log files with topics as file names

- documentation
- demo app
