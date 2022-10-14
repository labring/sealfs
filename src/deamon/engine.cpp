#include <deamon/engine.hpp>
#include <common/logging.hpp>
#include <deamon/util.hpp>
#include <unistd.h>

// reset data for test
void Engine::init() {

    /* clean all data */
    leveldb::Iterator* it = file_attr_db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        file_attr_db->Delete(leveldb::WriteOptions(), it->key());
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;

    it = sub_dir_db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        sub_dir_db->Delete(leveldb::WriteOptions(), it->key());
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;

    it 	= file_db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        file_db->Delete(leveldb::WriteOptions(), it->key());
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;

    /* add root directory */
    leveldb::Status status = file_attr_db->Put(leveldb::WriteOptions(), "/", "d");
    assert(status.ok());
    status = sub_dir_db->Put(leveldb::WriteOptions(), "/", add_dir(add_dir("","."), ".."));
    assert(status.ok());
    status = file_db->Put(leveldb::WriteOptions(), "/", "");
    assert(status.ok());

    /* add additional files and directories*/
    
}

Engine::Engine() {
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "testdb", &file_attr_db);
    if (!status.ok()) {
        printf("Error opening database");
        exit(1);
    }
    status = leveldb::DB::Open(options, "testdirdb", &sub_dir_db);
    if (!status.ok()) {
        printf("Error opening database");
        exit(1);
    }
    status = leveldb::DB::Open(options, "testfiledb", &file_db);
    if (!status.ok()) {
        printf("Error opening database");
        exit(1);
    }
}

Engine::~Engine() {
    if (file_attr_db != NULL) {
        delete file_attr_db;
    }
    if (sub_dir_db != NULL) {
        delete sub_dir_db;
    }
    if (file_db != NULL) {
        delete file_db;
    }
}

int Engine::create_file(leveldb::Slice path, mode_t mode) {


    std::string value;
    if (path.data()[path.size()-1] == '/') {
        LOG("create dir %s, not regular file", path.data());
        return -EISDIR;
    }
    leveldb::Status status = file_attr_db->Get(read_options, path, &value);
    if (status.ok()) {
        LOG("file %s already exists", path.data());
        return -EEXIST;
    }
    if (!status.IsNotFound()) {
        LOG("error when get file %s", path.data());
        return -EIO;
    }

    /* create parent dir info */
    int parent_path_length = 0;
    for (int i = path.size() - 1; i >= 0; i--) {
        if (path.data()[i] == '/') {
            parent_path_length = i+1;
            break;
        }
    }
    if (parent_path_length == 0) {
        LOG("error when get parent dir of %s", path.data());
        return -EIO;
    }
    leveldb::Slice parent_key(path.data(), parent_path_length);
    status = sub_dir_db->Get(read_options, parent_key, &value);
    if (!status.ok()) {
        LOG("error when get parent dir %s", std::string(parent_key.data(), parent_key.size()).c_str());
        return -ENOENT;
    }
    std::string new_dir = std::string(path.data() + parent_path_length, path.size() - parent_path_length);
    LOG("new_dir %s", new_dir.c_str());
    status = sub_dir_db->Put(write_options, parent_key, add_dir(value, new_dir));
    if (!status.ok()) {
        LOG("error when update parent dir %s", parent_key.data());
        return -EIO;
    }

    /* create local file data */
    status = file_attr_db->Put(write_options, path, "f");
    if (!status.ok()) {
        LOG("error when create file %s", path.data());
        return -EIO;
    }
    std::string local_file_name = grenerate_local_file_name(path);
    status = file_db->Put(write_options, path, local_file_name);
    if (!status.ok()) {
        LOG("error when create file %s", path.data());
        return -EIO;
    }
    std::string dir_path = local_file_name.substr(0, local_file_name.find_last_of("/"));
    // create local directory
    if (access(dir_path.c_str(), F_OK) == -1) {
        if (mkdir(dir_path.c_str(), 0777) == -1) {
            LOG("error when create local directory %s", dir_path.c_str());
            return -EIO;
        }
    }
    // create local file
    int fd = open(local_file_name.c_str(), O_CREAT | O_RDWR, mode);
    if (fd < 0) {
        LOG("error when create local file %s", local_file_name.c_str());
        return -EIO;
    }
    close(fd);

    return 0;
}

int Engine::create_dir(leveldb::Slice path, mode_t mode) {
    std::string value;
    if (path.data()[path.size()-1] != '/') {
        LOG("create file %s, not dir", path.data());
        return -ENOTDIR;
    }
    leveldb::Status status = file_attr_db->Get(read_options, path, &value);
    if (status.ok()) {
        LOG("dir %s already exists", path.data());
        return -EEXIST;
    }
    if (!status.IsNotFound()) {
        LOG("error when get dir %s", path.data());
        return -EIO;
    }

    /* create parent dir info */
    int parent_path_length = 0;
    for (int i = path.size() - 2; i >= 0; i--) {
        if (path.data()[i] == '/') {
            parent_path_length = i+1;
            break;
        }
    }
    if (parent_path_length == 0) {
        LOG("error when get parent dir of %s", path.data());
        return -EIO;
    }
    leveldb::Slice parent_key(path.data(), parent_path_length);
    status = sub_dir_db->Get(read_options, parent_key, &value);
    if (!status.ok()) {
        LOG("error when get parent dir %s", std::string(parent_key.data(), parent_key.size()).c_str());
        return -ENOENT;
    }
    std::string new_dirs = add_dir(value, std::string(path.data() + parent_path_length, path.size() - parent_path_length));
    LOG("new_dirs: %s", new_dirs.c_str());
    status = sub_dir_db->Put(write_options, parent_key, new_dirs);
    if (!status.ok()) {
        LOG("error when update parent dir %s", std::string(parent_key.data(), parent_key.size()).c_str());
        return -EIO;
    }
    status = sub_dir_db->Put(write_options, path, add_dir(add_dir("","."), ".."));
    if (!status.ok()) {
        LOG("error when create dir %s", path.data());
        return -EIO;
    }

    /* create dir */
    status = file_attr_db->Put(write_options, path, "d");
    if (!status.ok()) {
        LOG("error when create dir %s", path.data());
        return -EIO;
    }

    return 0;
}

int Engine::get_file_attr(leveldb::Slice path, struct stat *stbuf) {
    leveldb::Slice key(path.data(), path.size());
    std::string value;
    leveldb::Status status = file_attr_db->Get(read_options, key, &value);
    if (!status.ok()) {
        return -ENOENT;
    }
    if (value == "f") {
        stbuf->st_mode = S_IFREG | 0777;
        stbuf->st_nlink = 1;
    } else if (value == "d") {
        stbuf->st_mode = S_IFDIR | 0777;
        stbuf->st_nlink = 2;
    } else {
        return -ENOENT;
    }
    return 0;
}

int Engine::read_dir(leveldb::Slice path, void *buf, seal_size_t *size) {
    std::string value;
    leveldb::Status status = file_attr_db->Get(read_options, path, &value);
    if (!status.ok()) {
        return -ENOENT;
    }
    if (value != "d") {
        return -ENOTDIR;
    }
    status = sub_dir_db->Get(read_options, path, &value);
    if (!status.ok()) {
        return -ENOENT;
    }
    memcpy(buf, value.data(), value.size());
    *size = value.size();
    return 0;
}

int Engine::write_file(leveldb::Slice path, const void *buf, seal_size_t size, off_t offset) {
    leveldb::Slice key(path.data(), path.size());
    std::string value;
    leveldb::Status status = file_attr_db->Get(read_options, key, &value);
    if (!status.ok()) {
        LOG("error when get file attribute %s", path.data());
        return -ENOENT;
    }
    if (value != "f") {
        return -EISDIR;
    }
    status = file_db->Get(read_options, key, &value);
    if (!status.ok()) {
        LOG("error when get file %s", path.data());
        return -EIO;
    }
    std::string local_file_name = value;
    int fd = open(local_file_name.c_str(), O_WRONLY);
    if (fd < 0) {
        LOG("error when open file %s", local_file_name.c_str());
        return -EIO;
    }
    LOG("write file %s, %s, offset %ld, size %d", local_file_name.c_str(), std::string((char *)buf, size).c_str(), offset, size);
    int ret = pwrite(fd, buf, size, offset);
    if (ret < 0) {
        LOG("error when write file %s, errno %s", local_file_name.c_str(), strerror(errno));
        return -EIO;
    }
    close(fd);
    return size;
}

int Engine::read_file(leveldb::Slice path, void *buf, seal_size_t size, off_t offset) {
    leveldb::Slice key(path.data(), path.size());
    std::string value;
    leveldb::Status status = file_attr_db->Get(read_options, key, &value);
    if (!status.ok()) {
        LOG("error when get file attribute %s", path.data());
        return -ENOENT;
    }
    if (value != "f") {
        return -EISDIR;
    }
    status = file_db->Get(read_options, key, &value);
    if (!status.ok()) {
        LOG("error when get file %s", path.data());
        return -EIO;
    }
    std::string local_file_name = value;
    int fd = open(local_file_name.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG("error when open file %s", local_file_name.c_str());
        return -EIO;
    }
    int ret = pread(fd, buf, size, offset);
    if (ret < 0) {
        LOG("error when read file %s, error %s", local_file_name.c_str(), strerror(errno));
        return -EIO;
    }
    close(fd);
    return size;
}