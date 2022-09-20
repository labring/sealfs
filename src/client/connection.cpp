
#include <errno.h>
#include <client/connection.hpp>
#include <client/logging.hpp>
#include <common/protocol.hpp>
#include <stdlib.h>
#include <arpa/inet.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

Connection::Connection(const char* host, const char* port) {
    this->host = strdup(host);
    this->port = strdup(port);
    this->sock = -1;
    this->addr.sin_family = AF_INET;
    this->addr.sin_port = htons(atoi(port));
    this->addr.sin_addr.s_addr = inet_addr(host);
    this->connected = 0;

    this->callback_start = 0;
    this->callback_end = 0;
    this->callbacks = (OperationCallback*) malloc(sizeof(OperationCallback) * MAX_BUFFER_SIZE);
    for (int i = 0; i < MAX_BUFFER_SIZE; i++) {
        this->callbacks[i].lock = new std::mutex();
        this->callbacks[i].cond = new std::condition_variable();
        this->callbacks[i].status = (int*) malloc(sizeof(int));
        *(this->callbacks[i].status) = -1;
    }

    if (reconnect() < 0) {
        LOG("Failed to connect to %s:%s", host, port);
    }
}

Connection::~Connection() {
    free(this->host);
    free(this->port);
    close(this->sock);
    for (int i = 0; i < MAX_BUFFER_SIZE; i++) {
        if (this->callbacks[i].lock != NULL) {
            delete this->callbacks[i].lock;
        }
        if (this->callbacks[i].cond != NULL) {
            delete this->callbacks[i].cond;
        }
        if (this->callbacks[i].status != NULL) {
            delete this->callbacks[i].status;
        }
    }
    free(this->callbacks);
}

inline int Connection::reconnect() {
    if (this->connected) {
        return 0;
    }
    this->connect_lock.lock();
    this->sock = socket(AF_INET, SOCK_STREAM, 0);
    if (this->sock == -1) {
        LOG("Error creating socket");
        this->connect_lock.unlock();
        return -1;
    }
    if (connect(this->sock, (struct sockaddr *)&this->addr, sizeof(this->addr)) == -1) {
        LOG("Error connecting to server");
        this->connect_lock.unlock();
        return -1;
    }
    this->connected = 1;
    this->connect_lock.unlock();
    LOG("Connected to %s:%s", this->host, this->port);
    return 0;
}

inline void Connection::disconnect() {
    if (this->connected) {
        this->connect_lock.lock();
        if (this->connected) {
            close(this->sock);
            this->connected = 0;
            this->sock = -1;
        }
        this->connect_lock.unlock();
    }
}

/* receive operation response and wake up the operation thread using condition variable */

void Connection::recv_response() {
    while (true) {
        int status;
        int id;
        int size;
        char* buffer;
        if (recv(this->sock, &status, sizeof(int), 0) <= 0) {
            LOG("Error receiving response");
            disconnect();
            return;
        }
        if (recv(this->sock, &id, sizeof(int), 0) <= 0) {
            LOG("Error receiving response");
            disconnect();
            return;
        }
        if (recv(this->sock, &size, sizeof(int), 0) <= 0) {
            LOG("Error receiving response");
            disconnect();
            return;
        }
        if (size > 0) {
            buffer = (char*) malloc(size);
            if (recv(this->sock, buffer, size, 0) <= 0) {
                LOG("Error receiving response");
                disconnect();
                return;
            }
        } else {
            buffer = NULL;
        }
        //this->callbacks[id].lock->lock();
        this->callbacks[id].status = &status;
        this->callbacks[id].size = size;
        this->callbacks[id].buffer = buffer;
        //this->callbacks[id].lock->unlock();
        this->callbacks[id].cond->notify_one();
    }
}

int Connection::send_request(int id, int type, int flags, int total_length, int path_length, const char* path, int meta_data_length, const void* meta_data, int data_length, const void* data) {
    assert(total_length == sizeof(int) * 3 + path_length + meta_data_length + data_length);
    this->send_lock.lock();
    if (reconnect() < 0) {
        this->send_lock.unlock();
        return -1;
    }
    if (send(this->sock, &id, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -1;
    }
    if (send(this->sock, &type, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -1;
    }
    if (send(this->sock, &flags, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -1;
    }
    if (send(this->sock, &total_length, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -1;
    }
    if (send(this->sock, &path_length, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -1;
    }
    if (path_length>0) {
        if (send(this->sock, path, path_length, 0) <= 0) {
            LOG("Error sending request");
            this->send_lock.unlock();
            return -1;
        }
    }
    if (send(this->sock, &meta_data_length, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -1;
    }
    if (meta_data_length>0) {
        if (send(this->sock, meta_data, meta_data_length, 0) <= 0) {
            LOG("Error sending request");
            this->send_lock.unlock();
            return -1;
        }
    }
    if (send(this->sock, &data_length, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -1;
    }
    if (data_length>0) {
        if (send(this->sock, data, data_length, 0) <= 0) {
            LOG("Error sending request");
            this->send_lock.unlock();
            return -1;
        }
    }
    this->send_lock.unlock();
    return 0;
}


int Connection::create_remote_file(const char *path, mode_t mode)
{
    if (connected == 0) {
        return -EIO;
    }
    int id = this->callback_end;
    this->callback_end = (this->callback_end + 1) % MAX_BUFFER_SIZE;
    this->callbacks[id].lock->lock();
    int path_length = strlen(path) + 1;
    int total_length = sizeof(int) * 3 + path_length + sizeof(mode_t);
    int status = send_request(id, CREATE_FILE, 0, total_length, path_length, path, sizeof(mode_t), &mode, 0, NULL);
    if (status < 0) {
        this->callbacks[id].lock->unlock();
        return status;
    }
}

int Connection::create_remote_dir(const char *path, mode_t mode)
{
    if (connected == 0) {
        return -EIO;
    }
    return -EPERM;
}

int Connection::get_remote_file_attr(const char *path, struct stat *stbuf)
{
    if (connected == 0) {
        return -EIO;
    }
    return -EPERM;
}

int Connection::read_remote_dir(const char *path, void *buf, fuse_fill_dir_t filler)
{
    if (connected == 0) {
        return -EIO;
    }
    return -EPERM;
}

int Connection::open_remote_file(const char *path, struct fuse_file_info *fi)
{
    if (connected == 0) {
        return -EIO;
    }
    return -EPERM;
}

int Connection::read_remote_file(const char *path, char *buf, size_t size, off_t offset)
{
    if (connected == 0) {
        return -EIO;
    }
    return -EPERM;
}

int Connection::write_remote_file(const char *path, const char *buf, size_t size, off_t offset)
{
    if (connected == 0) {
        return -EIO;
    }
    return -EPERM;
}
