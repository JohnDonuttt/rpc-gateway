.PHONY: build test vet run

build:
	go build -o bin/rpc-gateway ./cmd/rpc-gateway

test:
	go test -race ./...

vet:
	gofmt -l .
	go vet ./...

run: build
	./bin/rpc-gateway -config examples/gateway.json
