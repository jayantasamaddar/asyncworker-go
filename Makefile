.PHONY: test build fmt vet lint

test:
	GOWORK=off go test ./... -race -v

build:
	GOWORK=off go build ./...

fmt:
	GOWORK=off go fmt ./...

vet:
	GOWORK=off go vet ./...

lint:
	GOWORK=off golangci-lint run
