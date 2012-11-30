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

## Run
To start a broker,

    $> go install octopi/run/broker
    $> bin/broker

  [websocket]: http://go.pkgdoc.org/code.google.com/p/go.net/websocket
