#include <deamon/server.hpp>
#include <common/logging.hpp>
#include <iostream>
#include <thread>

using namespace std;


Server::Server(int m_socket, Engine* m_engine) {
    sock = m_socket;
    connected = true;
    engine = m_engine;
}

Server::~Server() {
    if (connected) {
        disconnect();
    }
}

void Server::disconnect() {
    if (connected) {
        close(sock);
        connected = false;
    }
}

// requests
// | id | type | flags | total_length | filename_length | filename | meta_data_length | meta_data | data_length | data |
// | 4Byte | 4Byte | 4Byte | 4Byte | 4Byte | 1~4kB | 4Byte | 0~ | 4Byte | 0~ |

void Server::parse_request() {
    while (connected) {
        LOG("Waiting for request");
        char header[HEADER_SIZE];
        int read_size = recv(sock, header, HEADER_SIZE, MSG_WAITALL);
        if (read_size != HEADER_SIZE) {
            LOG("Error receiving request, read_size=%d errno=%d", read_size, errno);
            disconnect();
            return;
        }
        LOG("Received request");
        int id = *(int*) header;
        LOG("id: %d", id);
        OperationType type = *(OperationType*) (header + sizeof(int));
        LOG("type: %d", type);
        int flags = *(int*) (header + sizeof(int) * 2);
        LOG("flags: %d", flags);
        int total_length = *(int*) (header + sizeof(int) * 3);
        LOG("Received request: id=%d, type=%d, flags=%d, total_length=%d", id, type, flags, total_length);
        char* buffer = new char[total_length];
        if (recv(sock, buffer, total_length, MSG_WAITALL) != total_length) {
            LOG("Error receiving request");
            disconnect();
            return;
        }
        new std::thread(&Server::operation_filter, this, id, type, flags, total_length, buffer);
    }
    LOG("Server disconnected, parse request thread quits.");
}

void Server::operation_filter(int id, OperationType type, int flags, int total_length, char* buffer) {
    int path_length = *(int*) buffer;
    LOG("path_length: %d", path_length);

    off_t offset;
    size_t size;
    const char* data;

    // you can add custom filters here, do not forget recycle buffer memory

    switch (type) {
        case CREATE_FILE:
            LOG("dealing request: CREATE_FILE");
            create_file(id, leveldb::Slice(buffer + sizeof(int), path_length), *(mode_t*) (buffer + sizeof(int) + path_length + sizeof(int)));
            //new std::thread(&Server::create_file, this, id, leveldb::Slice(buffer + sizeof(int), path_length), *(mode_t*) (buffer + sizeof(int) + path_length + sizeof(int)));
            break;
        case CREATE_DIR:
            LOG("dealing request: CREATE_DIR");
            create_dir(id, leveldb::Slice(buffer + sizeof(int), path_length), *(mode_t*) (buffer + sizeof(int) + path_length + sizeof(int)));
            //new std::thread(&Server::create_dir, this, id, leveldb::Slice(buffer + sizeof(int), path_length), *(mode_t*) (buffer + sizeof(int) + path_length + sizeof(int)));
            break;
        case GET_FILE_ATTR:
            LOG("dealing request: GET_FILE_ATTR");
            get_file_attr(id, leveldb::Slice(buffer + sizeof(int), path_length));
            //new std::thread(&Server::get_file_attr, this, id, leveldb::Slice(buffer + sizeof(int), path_length));
            break;
        case READ_DIR:
            LOG("dealing request: READ_DIR");
            read_dir(id, leveldb::Slice(buffer + sizeof(int), path_length));
            //new std::thread(&Server::read_dir, this, id, leveldb::Slice(buffer + sizeof(int), path_length));
            break;
        case WRITE_FILE:
            LOG("dealing request: WRITE_FILE");
            offset = *(off_t*) (buffer + sizeof(int) + path_length + sizeof(int));
            size = *(int*) (buffer + sizeof(int) + path_length + sizeof(int) + sizeof(off_t));
            data = buffer + sizeof(int) + path_length + sizeof(int) + sizeof(off_t) + sizeof(int);
            write_file(id, leveldb::Slice(buffer + sizeof(int), path_length), data,  size, offset);
            //new std::thread(&Server::write_file, this, id, leveldb::Slice(buffer + sizeof(int), path_length), data, size, offset);
            break;
        case READ_FILE:
            LOG("dealing request: READ_FILE");
            offset = *(off_t*) (buffer + sizeof(int) + path_length + sizeof(int));
            size = *(size_t*) (buffer + sizeof(int) + path_length + sizeof(int) + sizeof(off_t));
            read_file(id, leveldb::Slice(buffer + sizeof(int), path_length), size, offset);
            //new std::thread(&Server::read_file, this, id, leveldb::Slice(buffer + sizeof(int), path_length), size, offset);
            break;
        //TODO
        default:
            LOG("Unknown request type %d", type);
            disconnect();
    }
    
    delete[] buffer;  // recycle buffer memory
}

int Server::response(int id, int status, int flags, int total_length, int meta_data_length, const void* meta_data, int data_length, const void* data) {
    LOG("Sending response");
    assert(total_length == meta_data_length + data_length);
    LOG("id=%d, status=%d, flags=%d, total_length=%d, meta_data_length=%d, data_length=%d", id, status, flags, total_length, meta_data_length, data_length);
    this->send_lock.lock();
    if (!connected) {
        return -1;
    }
    if (send(sock, &id, sizeof(int), 0) <= 0) {
        LOG("Error sending response");
        disconnect();
        return -1;
    }
    if (send(sock, &status, sizeof(int), 0) <= 0) {
        LOG("Error sending response");
        disconnect();
        return -1;
    }
    if (send(sock, &flags, sizeof(int), 0) <= 0) {
        LOG("Error sending response");
        disconnect();
        return -1;
    }
    if (send(sock, &total_length, sizeof(int), 0) <= 0) {
        LOG("Error sending response");
        disconnect();
        return -1;
    }
    if (send(sock, &meta_data_length, sizeof(int), 0) <= 0) {
        LOG("Error sending response");
        disconnect();
        return -1;
    }
    if (meta_data_length > 0) {
        if (send(sock, meta_data, meta_data_length, 0) <= 0) {
            LOG("Error sending response");
            disconnect();
            return -1;
        }
    }
    if (send(sock, &data_length, sizeof(int), 0) <= 0) {
            LOG("Error sending response");
            disconnect();
            return -1;
    }
    if (data_length > 0) {
        if (send(sock, data, data_length, 0) <= 0) {
            LOG("Error sending response");
            disconnect();
            return -1;
        }
    }
    this->send_lock.unlock();
    return 0;
}

void Server::create_file(int id, leveldb::Slice path, mode_t mode) {
    
    cout << "create_file" << endl;
    cout << "path: " << string(path.data(), path.size()) << endl;
    cout << "mode: " << mode << endl;

    assert(engine != NULL);

    int status = engine->create_file(path, mode);

    response(id, status, 0, 0, 0, NULL, 0, NULL);
}

void Server::create_dir(int id, leveldb::Slice path, mode_t mode) {
    
    cout << "create_dir" << endl;
    cout << "path: " << string(path.data(), path.size()) << endl;
    cout << "mode: " << mode << endl;

    assert(engine != NULL);

    int status = engine->create_dir(path, mode);

    response(id, status, 0, 0, 0, NULL, 0, NULL);
}

void Server::get_file_attr(int id, leveldb::Slice path) {
        
    cout << "get_file_attr" << endl;
    cout << "path: " << string(path.data(), path.size()) << endl;

    assert(engine != NULL);

    struct stat attr;
    
    int status = engine->get_file_attr(path, &attr);

    response(id, status, 0, sizeof(struct stat), sizeof(struct stat), &attr, 0, NULL);
}

void Server::read_dir(int id, leveldb::Slice path) {
    
    cout << "read_dir" << endl;
    cout << "path: " << string(path.data(), path.size()) << endl;

    assert(engine != NULL);

    char buf[MAX_BUFFER_SIZE];
    int size;

    int status = engine->read_dir(path, buf, &size);

    response(id, status, 0, size, size, buf, 0, NULL);
}

void Server::write_file(int id, leveldb::Slice path, const char* data, size_t size, off_t offset) {
    
    cout << "write_file" << endl;
    cout << "path: " << string(path.data(), path.size()) << endl;
    cout << "size: " << size << endl;
    cout << "offset: " << offset << endl;

    assert(engine != NULL);

    int status = engine->write_file(path, data, size, offset);

    response(id, status, 0, 0, 0, NULL, 0, NULL);
}

void Server::read_file(int id, leveldb::Slice path, size_t size, off_t offset) {
    
    cout << "read_file" << endl;
    cout << "path: " << string(path.data(), path.size()) << endl;
    cout << "size: " << size << endl;
    cout << "offset: " << offset << endl;

    assert(engine != NULL);

    char buf[MAX_BUFFER_SIZE];

    int status = engine->read_file(path, buf, size, offset);

    response(id, status, 0, size, size, buf, 0, NULL);
}