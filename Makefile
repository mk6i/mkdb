clean:
	rm -r -f data engine/data

run-cli:
	go run cmd/console/main.go

test:
	go test -v ./...