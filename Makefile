.PHONY: build test vet demo clean

build:
	go build -o bin/ ./cmd/...

test:
	go test -race -count=1 ./...

vet:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

demo:
	./scripts/demo.sh

clean:
	rm -rf bin data
