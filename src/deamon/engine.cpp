#include <deamon/engine.hpp>

Engine::Engine() {
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "testdb", &db);
    if (!status.ok()) {
        printf("Error opening database");
        exit(1);
    }
}

Engine::~Engine() {
    delete db;
}

int Engine::create_file(SimpleString path, mode_t mode) {
    leveldb::Slice key(path.data, path.length);
    std::string value;
    leveldb::Status status = db->Get(read_options, key, &value);
    if (status.ok()) {
        return -EEXIST;
    }
    status = db->Put(write_options, key, "file");
    if (!status.ok()) {
        return -EIO;
    }
    return 0;
}

int Engine::create_dir(SimpleString path, mode_t mode) {
    leveldb::Slice key(path.data, path.length);
    std::string value;
    leveldb::Status status = db->Get(read_options, key, &value);
    if (status.ok()) {
        return -EEXIST;
    }
    status = db->Put(write_options, key, "dir");
    if (!status.ok()) {
        return -EIO;
    }
    return 0;
}

int Engine::get_file_attr(SimpleString path, struct stat *stbuf) {
    leveldb::Slice key(path.data, path.length);
    std::string value;
    leveldb::Status status = db->Get(read_options, key, &value);
    if (!status.ok()) {
        return -ENOENT;
    }
    if (value == "file") {
        stbuf->st_mode = S_IFREG | 0777;
        stbuf->st_nlink = 1;
    } else if (value == "dir") {
        stbuf->st_mode = S_IFDIR | 0777;
        stbuf->st_nlink = 2;
    } else {
        return -ENOENT;
    }
    return 0;
}