#ifndef LOCAL_ENGINE_HPP
#define LOCAL_ENGINE_HPP

#include <common/fuse_version.hpp>
#include <common/types.hpp>
#include <fuse.h>
#include <leveldb/db.h>

class Engine {
public:
    Engine();
    ~Engine();
    void init();
    void run();
    void stop();

    int create_file(leveldb::Slice path, mode_t mode);
    int create_dir(leveldb::Slice path, mode_t mode);
    int get_file_attr(leveldb::Slice path, struct stat *stbuf);
    int read_dir(leveldb::Slice path, void *buf, seal_size_t *size);
    int open_file(leveldb::Slice path, struct fuse_file_info *fi);
    int read_file(leveldb::Slice path, void *buf, seal_size_t size, off_t offset);
    int write_file(leveldb::Slice path, const void *buf, seal_size_t size, off_t offset);
private:
    leveldb::DB* file_attr_db;
    leveldb::DB* sub_dir_db;
    leveldb::DB* file_db;
    leveldb::Options options;
    leveldb::WriteOptions write_options;
    leveldb::ReadOptions read_options;
};


#endif /* LOCAL_ENGINE_HPP */
