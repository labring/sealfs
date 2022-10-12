#include <common/fuse_version.hpp>
#include <fuse.h>
#include <leveldb/db.h>

#include <deamon/engine.hpp>
#include <common/protocol.hpp>
#include <common/types.hpp>

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

    int response(int id, int status, int flags, seal_size_t total_length, seal_size_t meta_data_length, const void* meta_data, seal_size_t data_length, const void* data);

    
    void operation_filter(int id, OperationType type, int flags, seal_size_t total_length, char* buf);

    void create_file(int id, leveldb::Slice path, mode_t mode);
    void create_dir(int id, leveldb::Slice path, mode_t mode);
    void get_file_attr(int id, leveldb::Slice path);
    void read_dir(int id, leveldb::Slice path);
    void open_file(int id, leveldb::Slice path, struct fuse_file_info *fi);
    void read_file(int id, leveldb::Slice path, seal_size_t size, off_t offset);
    void write_file(int id, leveldb::Slice path, const void* buf, seal_size_t size, off_t offset);
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

