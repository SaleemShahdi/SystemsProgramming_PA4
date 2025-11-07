CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -g
LDFLAGS = -lpthread -lrt

SRC = mcached.c
TARGET = mcached

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)