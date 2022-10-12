
#include <errno.h>
#include <client/connection.hpp>
#include <common/logging.hpp>
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
        this->callbacks[i].state = EMPTY;
    }

    if (reconnect() < 0) {
        LOG("Failed to connect to %s:%s", host, port);
    }

    this->recv_thread = new std::thread(&Connection::recv_response, this);
    this->recv_thread->detach();
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
        if (this->callbacks[i].data != NULL) {
            free(this->callbacks[i].data);
        }
        if (this->callbacks[i].meta_data != NULL) {
            free(this->callbacks[i].meta_data);
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

/* receive operation response and wake up the operation thread using condition variable
    response
    | id | status | flags | total_length | meta_data_lenght | meta_data | data_length | data |
    | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 0~ | 4Byte | 0~ |
*/

void Connection::recv_response() {
    while (true) {
        
        LOG("Waiting for response");
        if (!this->connected) {
            LOG("Connection to %s:%s lost", this->host, this->port);
            return;
        }

        /* read operation response */
        LOG("Reading response");
        char header[HEADER_SIZE];
        if (recv(this->sock, header, HEADER_SIZE, MSG_WAITALL) != HEADER_SIZE) {
            LOG("Error reading header %d", errno);
            this->disconnect();
            return;
        }

        /* parse header */
        LOG("Parsing response");
        int id = *(int*)header;
        int flags = *(int*)(header + sizeof(int) * 2);
        int total_length = *(int*)(header + sizeof(int) * 3);

        if (id < 0 || id >= MAX_BUFFER_SIZE) {
            LOG("Invalid id %d", id);
            this->disconnect();
            return;
        }
        
        /* check if the operation is outdated */
        LOG("Checking if the operation is outdated");
        if (this->callbacks[id].state != IN_PROGRESS) {
            LOG("Operation %d is outdated", id);
            char* buffer = (char*) malloc(total_length);
            if (recv(this->sock, buffer, total_length, MSG_WAITALL) != total_length) {
                LOG("Error reading data");
                this->disconnect();
                return;
            }
            free(buffer);
            continue;
        }

        this->callbacks[id].status = *(int*)(header + sizeof(int));

        LOG("Received response for operation id=%d, status=%d, flags=%d, total_length=%d", id, this->callbacks[id].status, flags, total_length);

        if (total_length < 0) {
            LOG("Invalid total length %d", total_length);
            this->disconnect();
            return;
        }

        /* read meta data */
        LOG("Reading meta data");
        int meta_data_length;
        if (recv(this->sock, &meta_data_length, sizeof(int), MSG_WAITALL) != sizeof(int)) {
            LOG("Error reading meta data length %d", errno);
            this->disconnect();
            return;
        }
        
        if (meta_data_length < 0) {
            LOG("Invalid meta data length %d", meta_data_length);
            this->disconnect();
            return;
        }

        if (meta_data_length > 0) {
            if (recv(this->sock, this->callbacks[id].meta_data, meta_data_length, MSG_WAITALL) != meta_data_length) {
                LOG("Error reading meta data %d", errno);
                this->disconnect();
                return;
            }
        }

        /* read data */
        LOG("Reading data");
        int data_length;
        if (recv(this->sock, &data_length, sizeof(int), MSG_WAITALL) != sizeof(int)) {
            LOG("Error reading data length");
            this->disconnect();
            return;
        }

        if (data_length < 0) {
            LOG("Invalid data length %d", data_length);
            this->disconnect();
            return;
        }

        if (data_length > 0) {
            if (recv(this->sock, this->callbacks[id].data, data_length, MSG_WAITALL) != data_length) {
                LOG("Error reading data");
                this->disconnect();
                return;
            }
        }

        this->callbacks[id].state = DONE;
        this->callbacks[id].cond->notify_one();
    }
}

int Connection::send_request(int id, int type, int flags, int total_length, int path_length, const char* path, int meta_data_length, const void* meta_data, int data_length, const void* data) {
    assert(total_length == sizeof(int) * 3 + path_length + meta_data_length + data_length);
    this->send_lock.lock();
    LOG("Sending request id=%d, type=%d, flags=%d, total_length=%d, path_length=%d, path=%s, meta_data_length=%d, data_length=%d", id, type, flags, total_length, path_length, path, meta_data_length, data_length);
    if (reconnect() < 0) {
        this->send_lock.unlock();
        return -EIO;
    }
    if (send(this->sock, &id, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -EIO;
    }
    if (send(this->sock, &type, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -EIO;
    }
    if (send(this->sock, &flags, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -EIO;
    }
    if (send(this->sock, &total_length, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -EIO;
    }
    if (send(this->sock, &path_length, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -EIO;
    }
    if (path_length>0) {
        if (send(this->sock, path, path_length, 0) <= 0) {
            LOG("Error sending request");
            this->send_lock.unlock();
            return -EIO;
        }
    }
    if (send(this->sock, &meta_data_length, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -EIO;
    }
    if (meta_data_length>0) {
        if (send(this->sock, meta_data, meta_data_length, 0) <= 0) {
            LOG("Error sending request");
            this->send_lock.unlock();
            return -EIO;
        }
    }
    if (send(this->sock, &data_length, sizeof(int), 0) <= 0) {
        LOG("Error sending request");
        this->send_lock.unlock();
        return -EIO;
    }
    if (data_length>0) {
        if (send(this->sock, data, data_length, 0) <= 0) {
            LOG("Error sending request");
            this->send_lock.unlock();
            return -EIO;
        }
    }
    this->send_lock.unlock();
    return 0;
}


int Connection::create_remote_file(const char *path, mode_t mode)
{
    LOG("create_remote_file %s", path);
    if (connected == 0) {
        return -EIO;
    }
    int id = this->callback_end;
    this->callback_end = (this->callback_end + 1) % MAX_BUFFER_SIZE;
    int path_length = strlen(path);
    int total_length = sizeof(int) * 3 + path_length + sizeof(mode_t);

    LOG("sending request");
    int status = send_request(id, CREATE_FILE, 0, total_length, path_length, path, sizeof(mode_t), &mode, 0, NULL);
    if (status < 0) {
        return status;
    }
    LOG("waiting for response");
    this->callbacks[id].state = IN_PROGRESS;
    std::unique_lock<std::mutex> lock(*this->callbacks[id].lock);
    this->callbacks[id].cond->wait_for(lock, std::chrono::milliseconds(3000)); // TODO: http://events.jianshu.io/p/53902d400dab
    if (this->callbacks[id].state != DONE) {
        LOG("timeout");
        this->callbacks[id].state = EMPTY;
        return -ETIMEDOUT;
    }
    LOG("got response");
    status = this->callbacks[id].status;
    return status;
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
    LOG("get_remote_file_attr");
    if (connected == 0) {
        LOG("not connected");
        return -EIO;
    }

    int id = this->callback_end;
    this->callback_end = (this->callback_end + 1) % MAX_BUFFER_SIZE;
    this->callbacks[id].state = IN_PROGRESS;
    this->callbacks[id].meta_data = stbuf;


    LOG("sending request");
    int path_length = strlen(path);
    int total_length = sizeof(int) * 3 + path_length;
    int status = send_request(id, GET_FILE_ATTR, 0, total_length, path_length, path, 0, NULL, 0, NULL);
    if (status < 0) {
        this->callbacks[id].state = EMPTY;
        LOG("error sending request");
        return status;
    }
    
    LOG("waiting for response");
    std::unique_lock<std::mutex> lock(*this->callbacks[id].lock);
    this->callbacks[id].cond->wait_for(lock, std::chrono::milliseconds(3000));
    if (this->callbacks[id].state != DONE) {
        LOG("timeout");
        return -ETIMEDOUT;
    }
    LOG("got response");
    
    status = this->callbacks[id].status;
    return status;
}

int Connection::read_remote_dir(const char *path, void *buf, fuse_fill_dir_t filler)
{
    LOG("read_remote_dir");
    if (connected == 0) {
        LOG("not connected");
        return -EIO;
    }
    int id = this->callback_end;
    this->callback_end = (this->callback_end + 1) % MAX_BUFFER_SIZE;
    this->callbacks[id].state = IN_PROGRESS;
    this->callbacks[id].data = new char[MAX_DIR_LIST_BUFFER_SIZE];

    LOG("sending request");
    int path_length = strlen(path);
    int total_length = sizeof(int) * 3 + path_length;
    int status = send_request(id, READ_DIR, 0, total_length, path_length, path, 0, NULL, 0, NULL);
    if (status < 0) {
        LOG("error sending request");
        this->callbacks[id].state = EMPTY;
        free(this->callbacks[id].data);
        return status;
    }

    LOG("waiting for response");
    std::unique_lock<std::mutex> lock(*this->callbacks[id].lock);
    this->callbacks[id].cond->wait_for(lock, std::chrono::milliseconds(3000));
    if (this->callbacks[id].state != DONE) {
        LOG("timeout");
        this->callbacks[id].state = EMPTY;
        free(this->callbacks[id].data);
        return -ETIMEDOUT;
    }
    LOG("got response");
    status = this->callbacks[id].status;
    if (status < 0) {
        free(this->callbacks[id].data);
        this->callbacks[id].state = EMPTY;
        return status;
    }
    /* read dir from data in callback. Every object include name length and directory name. Stop while name length is zero*/
    LOG("reading dir");
    char *data = (char *)this->callbacks[id].data;
    for (int i = 0; i < MAX_DIR_LIST_BUFFER_SIZE;) {
        if (data[i] == 0) {
            break;
        }
        char name[MAX_BUFFER_SIZE];
        int start_index = i;
        for (i = start_index; i < MAX_DIR_LIST_BUFFER_SIZE; i++) {
            if (data[i] == ';') {
                break;
            }
        }
        memcpy(name, (char*)this->callbacks[id].data + i, i - start_index);
        name[i - start_index] = '\0';
        filler(buf, name, NULL, 0, FUSE_FILL_DIR_PLUS);
    }
    LOG("done reading dir");
    free(this->callbacks[id].data);
    LOG("freeing data");
    return 0;
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
    LOG("read_remote_file");
    if (connected == 0) {
        LOG("not connected");
        return -EIO;
    }
    int id = this->callback_end;
    this->callback_end = (this->callback_end + 1) % MAX_BUFFER_SIZE;
    this->callbacks[id].state = IN_PROGRESS;
    this->callbacks[id].data = new char[size];

    LOG("sending request");
    int path_length = strlen(path);
    int total_length = sizeof(int) * 4 + path_length;
    int status = send_request(id, READ_FILE, 0, total_length, path_length, path, sizeof(off_t), &offset, sizeof(size_t), &size);
    if (status < 0) {
        LOG("error sending request");
        this->callbacks[id].state = EMPTY;
        free(this->callbacks[id].data);
        return status;
    }

    LOG("waiting for response");
    std::unique_lock<std::mutex> lock(*this->callbacks[id].lock);
    this->callbacks[id].cond->wait_for(lock, std::chrono::milliseconds(3000));
    if (this->callbacks[id].state != DONE) {
        LOG("timeout");
        this->callbacks[id].state = EMPTY;
        free(this->callbacks[id].data);
        return -ETIMEDOUT;
    }
    LOG("got response");
    status = this->callbacks[id].status;
    if (status < 0) {
        free(this->callbacks[id].data);
        this->callbacks[id].state = EMPTY;
        return status;
    }
    memcpy(buf, this->callbacks[id].data, size);
    free(this->callbacks[id].data);
    return size;
}

int Connection::write_remote_file(const char *path, const char *buf, size_t size, off_t offset)
{
    LOG("write_remote_file");
    if (connected == 0) {
        LOG("not connected");
        return -EIO;
    }
    int id = this->callback_end;
    this->callback_end = (this->callback_end + 1) % MAX_BUFFER_SIZE;
    this->callbacks[id].state = IN_PROGRESS;

    LOG("sending request");
    int path_length = strlen(path);
    int total_length = sizeof(int) * 3 + path_length + size;
    int status = send_request(id, WRITE_FILE, 0, total_length, path_length, path,  0, NULL, size, buf);
    if (status < 0) {
        LOG("error sending request");
        this->callbacks[id].state = EMPTY;
        return status;
    }

    LOG("waiting for response");
    std::unique_lock<std::mutex> lock(*this->callbacks[id].lock);
    this->callbacks[id].cond->wait_for(lock, std::chrono::milliseconds(3000));
    if (this->callbacks[id].state != DONE) {
        LOG("timeout");
        this->callbacks[id].state = EMPTY;
        return -ETIMEDOUT;
    }
    LOG("got response");
    status = this->callbacks[id].status;
    return status;
}