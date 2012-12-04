.PHONY: install
install:
	go install octopi/run/broker
	go install octopi/run/register
	go install octopi/run/producer
	go test octopi/util/config -i
	go test octopi/api/protocol -i
	go test octopi/impl/producer -i

.PHONY: test
test:
	go test octopi/util/config -v
	go test octopi/api/protocol -v
	go test octopi/impl/producer -v
