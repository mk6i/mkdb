clean:
	rm -r -f data engine/data

run-cli:
	go run ./cmd/console

test:
	go test -v ./...