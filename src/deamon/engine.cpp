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

inline int add_directory(SimpleString path, void *buf, int offset) {
    memcpy(buf + offset, (void*)&path.length, sizeof(int));
    offset += sizeof(int);
    memcpy(buf + offset, (void*)path.data, path.length);
    offset += path.length;
    return offset;
}

int Engine::read_dir(SimpleString path, void *buf) {
    leveldb::Slice key(path.data, path.length);
    std::string value;
    leveldb::Status status = db->Get(read_options, key, &value);
    if (!status.ok()) {
        return -ENOENT;
    }
    if (value != "dir") {
        return -ENOTDIR;
    }
    leveldb::Iterator* it = db->NewIterator(read_options);
    int offset = 0;
    for (it->Seek(key); it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        if (key.size() <= path.length) {
            break;
        }
        if (memcmp(key.data(), path.data, path.length) != 0) {
            break;
        }
        if (key.data()[path.length] != '/') {
            break;
        }
        SimpleString subpath;
        subpath.data = key.data() + path.length + 1;
        subpath.length = key.size() - path.length - 1;
        for (int i = 0; i < subpath.length; i++) {
            if (subpath.data[i] == '/') {
                subpath.length = i;
                break;
            }
        }
        offset = add_directory(subpath, buf, offset);
    }
    delete it;
    return 0;
}