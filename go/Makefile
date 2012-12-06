PACKAGES = octopi/util/config octopi/api/protocol octopi/impl/producer octopi/impl/brokerimpl octopi/demo/twitprod

.PHONY: install
install:
	go install octopi/run/broker
	go install octopi/run/register
	go install octopi/run/producer
	go test -i $(PACKAGES)

.PHONY: test
test: install
	for package in $(PACKAGES); do go test -v $$package; done
