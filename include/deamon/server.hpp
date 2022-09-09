#include <client/fuse_version.hpp>
#include <fuse.h>
#include "leveldb/db.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <mutex>

class Server {
public:
    Server(int m_socket);
    ~Server();
    inline void disconnect();

    void parse_request();

    int response(const void* buf, int size);

    void create_file(const char *path, mode_t mode, uid_t uid);
    int create_dir(const char *path, mode_t mode);
    int get_file_attr(const char *path, struct stat *stbuf);
    int read_dir(const char *path, void *buf, fuse_fill_dir_t filler);
    int open_file(const char *path, struct fuse_file_info *fi);
    int read_file(const char *path, char *buf, size_t size, off_t offset);
    int write_file(const char *path, const char *buf, size_t size, off_t offset);
    int sock;
    volatile char connected;
private:
    //sockaddr_in addr;
    //char* host;
    //char* port;
    std::mutex connect_lock;
    time_t last_request;

    leveldb::DB* db;
    leveldb::Options options;
};

