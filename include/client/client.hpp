#include <client/connection.hpp>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#define MAX_CONNECTIONS 1024
/* class for holding all active connections */

/* ConsistencyHashing Node */

class Node {
public:
    char* host;
    char* port;
    Node(const char* host, const char* port);
    ~Node();
    inline Connection* get_connection();
    inline void disconnect();
//private:
    Connection* connection;
};


class Client {
public:
    Client();
    ~Client();

    void add_node(const char* host, const char* port);

    inline Connection* get_connection(int index);

    /* map path to host and port index*/
    int map_path(const char* path);

    int add_server(const char* host, const char* port);
    int delete_server(int index);

    int delete_connection(int index);
    
    int create_remote_file(const char *path, mode_t mode);
    int create_remote_dir(const char *path, mode_t mode);
    int get_remote_file_attr(const char *path, struct stat *stbuf);
    int read_remote_dir(const char *path, void *buf, fuse_fill_dir_t filler);
    int open_remote_file(const char *path, struct fuse_file_info *fi);
    int read_remote_file(const char *path, char *buf, size_t size, off_t offset);
    int write_remote_file(const char *path, const char *buf, size_t size, off_t offset);
private:
    std::map<int, Node*> server_list;
};

extern std::mutex client_write_lock;

extern Client* client;

Client* get_client();