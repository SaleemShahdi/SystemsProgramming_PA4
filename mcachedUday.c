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

#define VERSION_STRING "C-Memcached 1.0"

int server_socket;
pthread_mutex_t global_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct entry_t {
    uint8_t *key;
    size_t key_len;
    uint8_t *value;
    size_t value_len;
    pthread_mutex_t lock;
    UT_hash_handle hh;
} entry_t;

entry_t *hash_table = NULL;

// EDIT: send_all helper to ensure full transmission
ssize_t send_all(int sock, const void *buf, size_t len) {
    const uint8_t *ptr = buf;
    size_t total = 0;
    while (total < len) {
        ssize_t n = send(sock, ptr + total, len - total, 0);
        if (n <= 0) return -1;
        total += n;
    }
    return total;
}

int generateResponse(int client_fd, memcache_req_header_t *header, uint8_t *body) {
    switch (header->opcode) {

        case CMD_GET: {
            printf("Processing CMD_GET\n");
            uint16_t key_len = ntohs(header->key_length);
            uint8_t *key = body;

            entry_t *entry = NULL;
            pthread_mutex_lock(&global_lock);
            HASH_FIND(hh, hash_table, key, key_len, entry);
            pthread_mutex_unlock(&global_lock);

            if (!entry) {
                memcache_req_header_t res = {0};
                res.magic = 0x81;
                res.opcode = CMD_GET;
                res.vbucket_id = htons(RES_NOT_FOUND);
                res.total_body_length = htonl(0);
                if (send_all(client_fd, &res, sizeof(res)) < 0) {
                    perror("send_all (GET not found) failed");
                }
                return 0;
            }

            pthread_mutex_lock(&entry->lock);

            // Prepare the response header
            memcache_req_header_t res = {0};
            res.magic = 0x81;
            res.opcode = CMD_GET;
            res.vbucket_id = htons(RES_OK);
            res.total_body_length = htonl(entry->value_len);

            // Allocate a single buffer for header + value
            size_t total_len = sizeof(res) + entry->value_len;
            uint8_t *out = malloc(total_len);
            if (!out) {
                perror("malloc failed");
                pthread_mutex_unlock(&entry->lock);
                return -1;
            }

            memcpy(out, &res, sizeof(res));
            memcpy(out + sizeof(res), entry->value, entry->value_len);

            if (send_all(client_fd, out, total_len) < 0) {
                perror("send_all (GET header+value) failed");
            }

            free(out);
            pthread_mutex_unlock(&entry->lock);
            return 0;
        }

        case CMD_SET: {
            printf("Processing CMD_SET\n");
            uint16_t key_len = ntohs(header->key_length);
            uint32_t total_len = ntohl(header->total_body_length);
            uint32_t val_len = total_len - key_len;

            uint8_t *key = body;
            uint8_t *value = body + key_len;

            entry_t *entry = NULL;

            pthread_mutex_lock(&global_lock);
            HASH_FIND(hh, hash_table, key, key_len, entry);

            if (!entry) {
                entry = calloc(1, sizeof(entry_t)); // EDIT: calloc instead of malloc
                entry->key = malloc(key_len);
                memcpy(entry->key, key, key_len);
                entry->key_len = key_len;
                pthread_mutex_init(&entry->lock, NULL);
                HASH_ADD_KEYPTR(hh, hash_table, entry->key, key_len, entry);
            }
            pthread_mutex_unlock(&global_lock); 

            pthread_mutex_lock(&entry->lock);
            free(entry->value);
            entry->value = malloc(val_len);
            memcpy(entry->value, value, val_len);
            entry->value_len = val_len;
            pthread_mutex_unlock(&entry->lock);

            memcache_req_header_t res = {0};
            res.magic = 0x81;
            res.opcode = CMD_SET;
            res.vbucket_id = htons(RES_OK);
            res.total_body_length = htonl(0);
            if (send_all(client_fd, &res, sizeof(res)) < 0) { //edit
                perror("send_all failed");
            }
            
            return 0;
        }

        case CMD_ADD: {
            printf("Processing CMD_ADD\n");
            uint16_t key_len = ntohs(header->key_length);
            uint32_t total_len = ntohl(header->total_body_length);
            uint32_t val_len = total_len - key_len;

            uint8_t *key = body;
            uint8_t *value = body + key_len;

            entry_t *entry = NULL;

            pthread_mutex_lock(&global_lock);
            HASH_FIND(hh, hash_table, key, key_len, entry);

            if (entry) {
                pthread_mutex_unlock(&global_lock);
                memcache_req_header_t res = {0};
                res.magic = 0x81;
                res.opcode = CMD_ADD;
                res.vbucket_id = htons(RES_EXISTS);
                res.total_body_length = htonl(0);
                if (send_all(client_fd, &res, sizeof(res)) < 0) { //edit
                    perror("send_all failed");
                }
                return 0;
            }

            entry = calloc(1, sizeof(entry_t)); // EDIT: calloc instead of malloc
            entry->key = malloc(key_len);
            memcpy(entry->key, key, key_len);
            entry->key_len = key_len;
            pthread_mutex_init(&entry->lock, NULL);
            HASH_ADD_KEYPTR(hh, hash_table, entry->key, key_len, entry);
            pthread_mutex_unlock(&global_lock);  

            pthread_mutex_lock(&entry->lock);
            entry->value = malloc(val_len);
            memcpy(entry->value, value, val_len);
            entry->value_len = val_len;
            pthread_mutex_unlock(&entry->lock);

            memcache_req_header_t res = {0};
            res.magic = 0x81;
            res.opcode = CMD_ADD;
            res.vbucket_id = htons(RES_OK);
            res.total_body_length = htonl(0);
            if (send_all(client_fd, &res, sizeof(res)) < 0) { //edit
                perror("send_all failed");
            }
            return 0;
        }

        case CMD_DELETE: {
            printf("Processing CMD_DELETE\n");
            uint16_t key_len = ntohs(header->key_length);
            uint8_t *key = body;

            entry_t *entry = NULL;

            pthread_mutex_lock(&global_lock);
            HASH_FIND(hh, hash_table, key, key_len, entry);

            if (!entry) {
                pthread_mutex_unlock(&global_lock);
                memcache_req_header_t res = {0};
                res.magic = 0x81;
                res.opcode = CMD_DELETE;
                res.vbucket_id = htons(RES_NOT_FOUND);
                res.total_body_length = htonl(0);
                if (send_all(client_fd, &res, sizeof(res)) < 0) { //edit
                    perror("send_all failed");
                }
                return 0;
            }

            pthread_mutex_lock(&entry->lock);
            HASH_DEL(hash_table, entry);
            pthread_mutex_unlock(&entry->lock);
            pthread_mutex_destroy(&entry->lock);
            free(entry->key);
            free(entry->value);
            free(entry);
            pthread_mutex_unlock(&global_lock);

            memcache_req_header_t res = {0};
            res.magic = 0x81;
            res.opcode = CMD_DELETE;
            res.vbucket_id = htons(RES_OK);
            res.total_body_length = htonl(0);
            if (send_all(client_fd, &res, sizeof(res)) < 0) { //edit
                perror("send_all failed");
            }
            return 0;
        }

        case CMD_VERSION: {
            printf("Processing CMD_VERSION\n");
            const char *ver = VERSION_STRING;
            size_t len = strlen(ver);

            memcache_req_header_t res = {0};
            res.magic = 0x81;
            res.opcode = CMD_VERSION;
            res.vbucket_id = htons(RES_OK);
            res.total_body_length = htonl(len);

            // Allocate a single buffer for header + version string
            size_t total_len = sizeof(res) + len;
            uint8_t *out = malloc(total_len);
            if (!out) {
                perror("malloc failed");
                return -1;
            }

            memcpy(out, &res, sizeof(res));
            memcpy(out + sizeof(res), ver, len);

            if (send_all(client_fd, out, total_len) < 0) {
                perror("send_all (VERSION header+value) failed");
            }

            free(out);
            return 0;
        }

        default: {
            memcache_req_header_t res = {0};
            res.magic = 0x81;
            res.opcode = header->opcode;
            res.vbucket_id = htons(RES_ERROR);
            res.total_body_length = htonl(0);
            if (send_all(client_fd, &res, sizeof(res)) < 0) { //edit
                    perror("send_all failed");
            }
            return 0;
        }
    }

    return 0;
}

void handle_client(int client_fd) {
    while (1) {
        memcache_req_header_t header;
        ssize_t n = recv(client_fd, &header, sizeof(header), MSG_WAITALL);
        if (n <= 0 || n != sizeof(header)) {
            break;  // client closed connection or error
        }

        if (header.magic != 0x80) {
            fprintf(stderr, "Invalid magic number: 0x%02x\n", header.magic);
            break;
        }

        uint32_t body_len = ntohl(header.total_body_length);
        uint8_t *body = malloc(body_len);
        if (!body || recv(client_fd, body, body_len, MSG_WAITALL) != body_len) {
            perror("Body Receive Error");
            free(body);
            break;
        }

        generateResponse(client_fd, &header, body);
        free(body);
    }

    close(client_fd);
}

void *worker_thread() {
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_socket, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }
        handle_client(client_fd);
    }
    return NULL;
}

int main(int argc, char **argv){
    if (argc != 3) {
        fprintf(stderr, "Usage: %s port numThreads\n", argv[0]);
        exit(1);
    }

    unsigned short port = atoi(argv[1]);
    int numThreads = atoi(argv[2]);

    struct sockaddr_in server_addr;
    if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Socket()");
        exit(2);
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind()");
        exit(3);
    }

    if (listen(server_socket, 1000) < 0) {
        perror("Listen()");
        exit(4);
    }

    pthread_t threads[numThreads];
    for (int i = 0; i < numThreads; ++i) {
        if (pthread_create(&threads[i], NULL, worker_thread, NULL) != 0) {
            perror("pthread_create");
            exit(EXIT_FAILURE);
        }
    }

    for (int i = 0; i < numThreads; ++i) {
        pthread_join(threads[i], NULL);
    }

    close(server_socket);
    return 0;
}