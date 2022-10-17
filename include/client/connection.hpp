#ifndef CONNECT_HPP
#define CONNECT_HPP

#include <common/fuse_version.hpp>
#include <common/protocol.hpp>
#include <common/types.hpp>
#include <fuse.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <mutex>
#include <condition_variable>
#include <thread>

struct OperationCallback {
    void* data;
    void* meta_data;
    int data_length;
    int meta_data_length;
    seal_size_t size;
    std::mutex* lock;
    std::condition_variable* cond;
    CallbackState state;
    int status;
    time_t start_time;
};

class Connection {
public:
    Connection(const char* host, const char* port);
    ~Connection();
    inline int reconnect();
    inline void disconnect();

    void recv_response();
    int send_request(int id, int type, int flags, seal_size_t total_length, seal_size_t path_length, const char* path, seal_size_t meta_data_length, const void* meta_data, seal_size_t data_length, const void* data);

    int create_remote_file(const char *path, mode_t mode);
    int create_remote_dir(const char *path, mode_t mode);
    int get_remote_file_attr(const char *path, struct stat *stbuf);
    int read_remote_dir(const char *path, void *buf, fuse_fill_dir_t filler);
    int open_remote_file(const char *path, struct fuse_file_info *fi);
    int read_remote_file(const char *path, char *buf, seal_size_t size, off_t offset);
    int write_remote_file(const char *path, const char *buf, seal_size_t size, off_t offset);
    int sock;
    volatile char connected;
private:
    sockaddr_in addr;
    char* host;
    char* port;
    std::mutex connect_lock;
    std::mutex send_lock;

    int callback_start, callback_end;
    OperationCallback* callbacks;

    std::thread* recv_thread;
};

#endif // CONNECT_HPP