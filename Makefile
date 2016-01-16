build:
	go build -v -o gorrd


cover:
	go test -v -cover -coverprofile cover.out
	go tool cover -html cover.out

