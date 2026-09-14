CC     = gcc
CFLAGS = -Wall -Wextra -Wpedantic -fPIC -O2 -g -Ivendor
SRCS   = cy_game.c vendor/flecs.c pathfinding.c physics.c goap.c
TARGET = cy_game.so

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
    LDFLAGS = -dynamiclib
else
    LDFLAGS = -shared
endif

all: check-vendor $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^
	@echo "Built $(TARGET)"

check-vendor:
	@test -f vendor/flecs.h      || (echo "Run: make fetch-headers" && exit 1)
	@test -f vendor/flecs.c      || (echo "Run: make fetch-headers" && exit 1)
	@test -f vendor/redismodule.h || (echo "Run: make fetch-headers" && exit 1)

fetch-headers:
	@mkdir -p vendor
	curl -sSL https://raw.githubusercontent.com/SanderMertens/flecs/master/distr/flecs.h \
	     -o vendor/flecs.h
	curl -sSL https://raw.githubusercontent.com/SanderMertens/flecs/master/distr/flecs.c \
	     -o vendor/flecs.c
	curl -sSL https://raw.githubusercontent.com/redis/redis/7.0/src/redismodule.h \
	     -o vendor/redismodule.h
	@echo "Vendor headers fetched."

clean:
	rm -f *.o $(TARGET)

.PHONY: all check-vendor fetch-headers clean
