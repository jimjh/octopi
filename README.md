# Octopi
One octopus, two octopi.

## Installation
This package depends on go's unofficial [websocket implementation][websocket].
To install the websockets package, execute the following:

    $> go get code.google.com/p/go.net/websocket

## Development
1. Clone the repository

  ```bash
  $> git clone git@github.com:jimjh/octopi.git
  ```

1. Ensure that your `$GOPATH` contains the `go` directory in this repository.

  ```sh
  # ~/.bash_profile
  export GOPATH="/path/to/git/repo/go"
  ```

## Testing
For unit tests, run

    $> cd go; make test

For integration tests, run

    $> tests/basictests.sh
    $> tests/transitiontests.sh

## Run
All paths below are relative to the `go` directory.

To start a broker,

    $> go install octopi/run/broker
    $> bin/broker -conf config/leader.json

To start a register,

    $> go install octopi/run/register
    $> bin/register -conf config/register.json

To start followers,

    $> go install octopi/run/broker
    $> bin/broker -conf config/follower1.json

Note that the leader/follower relationships are only for startup purposes. Once
the system is running, all brokers should join as followers. If the leader
dies, one of the followers will be elected to become the leader.

  [websocket]: http://go.pkgdoc.org/code.google.com/p/go.net/websocket
