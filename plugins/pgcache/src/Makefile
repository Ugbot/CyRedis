CC     = gcc
CFLAGS = -Wall -Wextra -fPIC -O2 -g -I.

PG_INC  = $(shell pg_config --includedir)
PG_LIB  = $(shell pg_config --libdir)
JAN_INC = $(shell pkg-config --cflags-only-I jansson 2>/dev/null)
JAN_LIB = $(shell pkg-config --libs jansson 2>/dev/null || echo "-ljansson")

CFLAGS  += -I$(PG_INC) $(JAN_INC)
LDFLAGS  = -L$(PG_LIB) -lpq $(JAN_LIB)

UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
    LDFLAGS += -dynamiclib
else
    LDFLAGS += -shared
endif

TARGET = pgcache.so

all: $(TARGET)

$(TARGET): pgcache.c redismodule.h
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ pgcache.c
	@echo "Built $(TARGET)"

clean:
	rm -f *.o $(TARGET)

.PHONY: all clean
