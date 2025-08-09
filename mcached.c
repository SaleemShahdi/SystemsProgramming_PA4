/* modifed code from
https://www.ibm.com/docs/en/zos/2.4.0?topic=programs-c-socket-tcp-server:
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include "uthash.h"
#include "mcached.h"
#include <linux/time.h>
/*
* Server Main.
*/
#define MSG_SIZE 32
typedef struct items {
    uint8_t key; /* key */
    uint8_t value;
    UT_hash_handle hh; /* makes this structure hashable */
} item;

item * hashTable = NULL;

char * generateResponse(uint8_t opcode, uint8_t * key, uint16_t key_length,
void * value2, uint16_t value_length, uint16_t vbucket_id) {
    uint32_t totalBodyLength = 0;
    if (opcode == CMD_VERSION) {
        char * value = (char *)(value2);
        totalBodyLength = key_length + strlen(value);
    } else {
        // uint8_t * value = (uint8_t *)(value); // ask what was going in this line of code. It made another pointer of type uint8_t with a different value for the pointer than that of the original pointer. This new pointer pointed to some other random location in memory. Ask how it was allowed to make a variable of the same name with a different type: wasn't allowed in the "add" function.
        uint8_t * value = (uint8_t *)(value2);
        totalBodyLength = key_length + value_length;
    }
    int totalLength = sizeof(memcache_req_header_t) + totalBodyLength;
    char buffer[sizeof(memcache_req_header_t)] = {0};
    memcache_req_header_t *hdr = (memcache_req_header_t *)buffer;
    hdr->opcode = opcode;
    hdr->magic = 0x81;
    hdr->key_length = key_length;
    hdr->vbucket_id = vbucket_id;
    hdr->total_body_length = totalBodyLength;
    char * response = malloc(totalLength);
    if (response == NULL) {
        perror("What are you doing with yourself?!?");
    }
    if (opcode == CMD_VERSION) {
        char * value = (char *)(value2);
        char * current = mempcpy(response, buffer, sizeof(memcache_req_header_t));
        // current = mempcpy(current, *key, key_length);
        current = mempcpy(current, value, strlen(value));
    } else {
        uint8_t * value = (uint8_t *)(value2);
        char * current = mempcpy(response, buffer, sizeof(memcache_req_header_t));
        current = mempcpy(current, key, key_length);
        current = mempcpy(current, value, value_length);
    }
    return response;



}
int get(int socket, uint8_t * key, uint16_t vbucket_id, uint16_t keyLength, uint16_t valueLength) {
    item * element;
    HASH_FIND(hh, hashTable, key, sizeof(*key), element);
    void * response = NULL;
    if (element == NULL) {
        // was here. Changin RES_NOT_FOUND and subsequent calls with constants to a pointer to a variable to accommodate for void pointer. Also segfaulting on mempcpy in generateResponse for some reason.
        int responseValue = RES_NOT_FOUND;
        response = generateResponse(CMD_GET, key, keyLength, &responseValue, sizeof(responseValue), vbucket_id);
    }
    else {
        response = generateResponse(CMD_GET, key, keyLength, &(element->value), sizeof(element->value), vbucket_id);
    }
    write(socket, response, sizeof(*response));
    return 1;
}

int set(int socket, uint8_t * key, uint8_t * value, uint16_t vbucket_id, uint16_t keyLength, uint16_t valueLength) {
    item * element;
    HASH_FIND(hh, hashTable, key, sizeof(*key), element);
    void * response = NULL;
    if (element == NULL) {
        element = malloc(sizeof(item));
        element->key = *key;
        element->value = *value;
        HASH_ADD(hh, hashTable, key, sizeof(*key), element);
        uint8_t responseValue = RES_OK;
        response = generateResponse(CMD_SET, key, keyLength, responseValue, sizeof(responseValue), vbucket_id);
    }
    else {
        element->value = *value;
        uint8_t responseValue = RES_OK;
        response = generateResponse(CMD_SET, key, keyLength, responseValue, sizeof(responseValue), vbucket_id);
    }
    write(socket, response, sizeof(*response));
    return 1;
}
int add(int socket, uint8_t * key, uint8_t * value, uint16_t vbucket_id, uint16_t keyLength, uint16_t valueLength) {
    item * element;
    HASH_FIND(hh, hashTable, key, keyLength, element);
    void * response = NULL;

    if (element == NULL) {
        element = malloc(sizeof(item));
        element->key = *key;
        element->value = *value;
        HASH_ADD(hh, hashTable, key, keyLength, element);
        uint8_t responseValue = RES_OK;
        uint8_t * test = &responseValue;
        void * test2 = (void *)(test);
        response = generateResponse(CMD_ADD, key, keyLength, &responseValue, sizeof(responseValue), vbucket_id);
    }
    else {
        uint8_t responseValue = RES_EXISTS;
        response = generateResponse(CMD_ADD, key, keyLength, &responseValue, sizeof(responseValue), vbucket_id);
    }
    write(socket, response, sizeof(*response));
    return 1;
}
int delete(int socket, uint8_t * key, uint16_t vbucket_id, uint16_t keyLength, uint16_t valueLength) {
    item * element;
    HASH_FIND(hh, hashTable, key, sizeof(*key), element);
    void * response = NULL;
    if (element == NULL) {
        uint8_t responseValue = RES_OK;
        response = generateResponse(CMD_DELETE, key, keyLength, &responseValue, sizeof(responseValue), vbucket_id);
    }
    else {
        HASH_DEL(hashTable, element);
        free(element);
        uint8_t responseValue = RES_OK;
        response = generateResponse(CMD_DELETE, key, keyLength, responseValue, sizeof(responseValue), vbucket_id);
    }
    write(socket, response, sizeof(*response));
    return 1;
}

int version(int socket, uint16_t vbucket_id, uint16_t keyLength, uint16_t valueLength) {
    void * response = NULL;
    response = generateResponse(CMD_VERSION, NULL, 0, "C-Memcached 1.0", sizeof("C-Memcached 1.0"), vbucket_id);

}
int output(int socket, uint16_t vbucket_id) {
    struct timespec ts;
    int a = clock_gettime(CLOCK_REALTIME, &ts);
    item * current = NULL;
    for (current = hashTable; current != NULL; current = current->hh.next) {
        
    }

}
/* very OG way to pass arguments in C -- name args then provide the types */
int main(int argc, char ** argv) {
unsigned short port; /* port server binds to */
char buf[MSG_SIZE]; /* buffer for sending & receiving data */
struct sockaddr_in client_addr; /* client address information */
struct sockaddr_in server_addr; /* server address information */
int orig_socket; /* socket for accepting connections */
int new_socket; /* socket connected to client */
int namelen; /* length of client name */
int sleep_time;
int keep_going; /* flag to keep the server accepting connections
from clients */
/*
* Check arguments. Should be only one: the port number to bind to.
*/
if (argc != 2)
{
fprintf(stderr, "Usage: %s port\n", argv[0]);
exit(1);
}
/*
* First argument should be the port.
*/
port = (unsigned short) atoi(argv[1]);
/*
* Get a socket for accepting connections.
*/
if ((orig_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0)
{
perror("Socket()");
exit(2);
}
/*
* Bind the socket to the server address.
*/
server_addr.sin_family = AF_INET;
server_addr.sin_port = htons(port);
server_addr.sin_addr.s_addr = INADDR_ANY;
if (bind(orig_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) <
0)
{
perror("Bind()");
exit(3);
}
/*
* Listen for connections. Specify the backlog as 1.
*/
if (listen(orig_socket, 1) != 0)
{
perror("Listen()");
exit(4);
}
/* add a while loop here to keep the server connecting to clients
*/
keep_going = 1 ; // flag I could change to zero to exit the server
while (keep_going ) {
/*
* Accept a connection.
*/
namelen = sizeof(client_addr);
if ((new_socket = accept(orig_socket, (struct sockaddr *)&client_addr, &namelen)) == -1) {
perror("Accept()");
exit(5);
}
/*
* Receive the message on the newly connected socket.
*/
/*if (recv(new_socket, buf, sizeof(buf), 0) == -1) {
perror("Recv()");
exit(6);
}*/

int headerSize = sizeof(memcache_req_header_t);
memcache_req_header_t header;
memcache_req_header_t * headerpointer = &header;
uint8_t * key = malloc(4);
uint8_t * value = malloc(10);
ssize_t n = recv(new_socket, headerpointer, headerSize, MSG_WAITALL);
header.key_length = ntohs(header.key_length);
header.total_body_length = ntohl(header.total_body_length);
// n is used if correct number of bytes is not read, handle later
uint8_t command = headerpointer->opcode;
switch (command) {
    case CMD_GET:
        n = recv(new_socket, key, header.key_length, MSG_WAITALL);
        get(new_socket, key, header.vbucket_id, header.key_length, (header.total_body_length - header.key_length));
        break;
    case CMD_SET:
        n = recv(new_socket, key, header.key_length, MSG_WAITALL);
        n = recv(new_socket, value, (header.total_body_length - header.key_length), MSG_WAITALL);
        set(new_socket, key, value, header.vbucket_id, header.key_length, (header.total_body_length - header.key_length));
        break;
    case CMD_ADD:
        n = recv(new_socket, key, header.key_length, MSG_WAITALL);
        n = recv(new_socket, value, (header.total_body_length - header.key_length), MSG_WAITALL);
        add(new_socket, key, value, header.vbucket_id, header.key_length, (header.total_body_length - header.key_length));
        // call add
        break;
    case CMD_DELETE:
        n = recv(new_socket, key, header.key_length, MSG_WAITALL);
        delete(new_socket, key, header.vbucket_id, header.key_length, (header.total_body_length - header.key_length));
        break;
    case CMD_VERSION:
        version(new_socket, header.vbucket_id, header.key_length, (header.total_body_length - header.key_length));
        // call version
        break;
    case CMD_OUTPUT:
        output(new_socket, header.vbucket_id);
        // call output
        break;
    default:
        // return generic error message (whatever that means)
    }
printf("Server got message: %s \n",buf);
/*
* Send the message back to the client.
*/
// sleep_time = 1;
// printf("the server is on a lunch break for %d seconds \n",sleep_time);
// sleep(sleep_time);
/*if (send(new_socket, buf, sizeof(buf), 0) < 0) {
perror("Send()");
exit(7);
}*/
/* hack so the OS reclaims the port sooner
Make the client close the socket first or the socket ends up in the
TIME_WAIT state trying to close
See: https://hea-www.harvard.edu/~fine/Tech/addrinuse.html
*/
// sleep(1);
close(new_socket);
} // end keep_going */
close(orig_socket);
sleep(1);
printf("Server ended successfully\n");
exit(0);
}