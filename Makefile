VERSION=`git describe --always`
DATE=`date`
LDFLAGS="-X main.AppVersion '$(VERSION) - $(DATE)'"

build:
	go build -v -o go-rrd
	
build_release:
	go build -v -o go-rrd -ldflags $(LDFLAGS)

build_pi:
	GOGCCFLAGS="-fPIC -O4 -Ofast -march=native -pipe -mcpu=arm1176jzf-s -mfpu=vfp -mfloat-abi=hard -s" \
		CHOST="armv6j-hardfloat-linux-gnueabi" \
		CXX=arm-linux-gnueabihf-g++ CC=arm-linux-gnueabihf-gcc \
		GOOS=linux GOARCH=arm GOARM=5 CGO_ENABLED=1 \
		go build -v --ldflags '-extldflags "-static"' --ldflags $(LDFLAGS) -o go-rrd-arm
	/usr/arm-linux-gnueabi/bin/strip go-rrd-arm

cover:
	go test -v -cover -coverprofile cover.out
	go tool cover -html cover.out

