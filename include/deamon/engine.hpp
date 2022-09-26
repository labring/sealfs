#ifndef LOCAL_ENGINE_HPP
#define LOCAL_ENGINE_HPP

#include <common/fuse_version.hpp>
#include <fuse.h>
#include <leveldb/db.h>

#include <common/struct/string.hpp>

class Engine {
public:
    Engine();
    ~Engine();
    void init();
    void run();
    void stop();

    int create_file(SimpleString path, mode_t mode);
    int create_dir(SimpleString path, mode_t mode);
    int get_file_attr(SimpleString path, struct stat *stbuf);
    int read_dir(SimpleString path, void *buf);
    int open_file(SimpleString path, struct fuse_file_info *fi);
    int read_file(SimpleString path, char *buf, size_t size, off_t offset);
    int write_file(SimpleString path, const void *buf, size_t size, off_t offset);
private:
    leveldb::DB* db;
    leveldb::Options options;
    leveldb::WriteOptions write_options;
    leveldb::ReadOptions read_options;
};


#endif /* LOCAL_ENGINE_HPP */
