clean:
	rm -r -f data engine/data

run-cli:
	go run cmd/console/main.go

run-csv-import:
	go run cmd/csvimport/main.go

test:
	go test -v ./...