#include <common/fuse_version.hpp>
#include <fuse.h>
#include <leveldb/db.h>

#include <deamon/engine.hpp>

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
    Server(int m_socket, Engine* m_engine);
    ~Server();
    inline void disconnect();

    void parse_request();

    // response
    // | id | status | flags | total_length | meta_data_lenght | meta_data | data_length | data |
    // | 8Byte | 8Byte | 8Byte | 8Byte | 8Byte | 0~ | 8Byte | 0~ |

    int response(int id, int status, int flags, int total_length, int meta_data_length, const void* meta_data, int data_length, const void* data);

    void create_file(int id, SimpleString path, mode_t mode);
    void create_dir(int id, SimpleString path, mode_t mode);
    void get_file_attr(int id, SimpleString path);
    void read_dir(int id, SimpleString path, void *buf, fuse_fill_dir_t filler);
    void open_file(int id, SimpleString path, struct fuse_file_info *fi);
    void read_file(int id, SimpleString path, char *buf, size_t size, off_t offset);
    void write_file(int id, SimpleString path, const char *buf, size_t size, off_t offset);
    int sock;
    sockaddr_in client;
    volatile char connected;
private:
    //sockaddr_in addr;
    //char* host;
    //char* port;
    std::mutex connect_lock;
    std::mutex send_lock;
    time_t last_request;

    Engine* engine;
};

