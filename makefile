CC = gcc
CFLAGS = -g -std=c99 -Wall -Wvla -lpthread -lrt -fsanitize=address,undefined
SRC = mcached
TARGET = mcached

$(TARGET): $(SRC).c
	$(CC) $(CFLAGS) -o $@ $^
	
client: client.c
	$(CC) $(CFLAGS) -o $@ $^

# Clean rule
clean:
	rm -rf $(TARGET) *.o *.a *.dylib *.dSYM

cleanclient:
	rm -rf client *.o *.a *.dylib *.dSYM
