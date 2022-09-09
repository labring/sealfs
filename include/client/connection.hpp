#ifndef CONNECT_HPP
#define CONNECT_HPP

#include <client/fuse_version.hpp>
#include <fuse.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <mutex>
#include <condition_variable>

#define MAX_BUFFER_SIZE 65535

struct OperationCallback {
    void* buffer;
    size_t size;
    std::mutex* lock;
    std::condition_variable* cond;
    int* status;
    char mode;
};

class Connection {
public:
    Connection(const char* host, const char* port);
    ~Connection();
    inline int reconnect();
    inline void disconnect();

    void recv_response();

    int create_remote_file(const char *path, mode_t mode);
    int create_remote_dir(const char *path, mode_t mode);
    int get_remote_file_attr(const char *path, struct stat *stbuf);
    int read_remote_dir(const char *path, void *buf, fuse_fill_dir_t filler);
    int open_remote_file(const char *path, struct fuse_file_info *fi);
    int read_remote_file(const char *path, char *buf, size_t size, off_t offset);
    int write_remote_file(const char *path, const char *buf, size_t size, off_t offset);
    int sock;
    volatile char connected;
private:
    sockaddr_in addr;
    char* host;
    char* port;
    std::mutex connect_lock;

    int callback_start, callback_end;
    OperationCallback* callbacks;
};


// void init_logger(const char* log_file_name);

// int create_remote_file(const char *path, mode_t mode);

// int create_remote_dir(const char *path, mode_t mode);

// int get_remote_file_attr(const char *path, struct stat *stbuf);

// int read_remote_dir(const char *path, void *buf, fuse_fill_dir_t filler);

// int open_remote_file(const char *path, struct fuse_file_info *fi);

// int read_remote_file(const char *path, char *buf, size_t size, off_t offset);

// int write_remote_file(const char *path, const char *buf, size_t size, off_t offset);

#endif // CONNECT_HPP