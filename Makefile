# libplctag paths — override these for your system
# After `sudo make install` of libplctag, these default to /usr/local
LIBPLCTAG_INCLUDE ?= /usr/local/include
LIBPLCTAG_LIB ?= /usr/local/lib

# For local builds from source (e.g. /tmp/libplctag/build/bin_dist)
# LIBPLCTAG_INCLUDE ?= /tmp/libplctag/build/bin_dist
# LIBPLCTAG_LIB ?= /tmp/libplctag/build/bin_dist

export CGO_CFLAGS = -I$(LIBPLCTAG_INCLUDE)
export CGO_LDFLAGS = -L$(LIBPLCTAG_LIB) -lplctag -Wl,-rpath,$(LIBPLCTAG_LIB)

.PHONY: build run clean

build:
	go build -o tentacle-ethernetip-go .

run: build
	./tentacle-ethernetip-go

clean:
	rm -f tentacle-ethernetip-go
