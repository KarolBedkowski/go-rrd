build:
	go build -v -o go-rrd

build_pi:
	GOGCCFLAGS="-fPIC -O4 -Ofast -march=native -s" CXX=arm-linux-gnueabi-g++ CC=arm-linux-gnueabi-gcc GOOS=linux GOARCH=arm GOARM=5 CGO_ENABLED=1 go build -v --ldflags '-extldflags "-static"' -o go-rrd-arm

cover:
	go test -v -cover -coverprofile cover.out
	go tool cover -html cover.out

