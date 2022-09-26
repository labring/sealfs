#include <deamon/server.hpp>
#include <common/logging.hpp>
#include <common/protocol.hpp>
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
            LOG("Error receiving request, read_size: %d", read_size);
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
        char buffer[total_length];
        if (recv(sock, buffer, total_length, MSG_WAITALL) != total_length) {
            LOG("Error receiving request");
            disconnect();
            return;
        }
        int path_length = *(int*) buffer;
        LOG("path_length: %d", path_length);

        switch (type) {
            case CREATE_FILE:
                //create_file(id, SimpleString(buffer + sizeof(int), path_length), *(mode_t*) (buffer + sizeof(int) + path_length));
                std::thread(&Server::create_file, this, id, SimpleString(buffer + sizeof(int), path_length), *(mode_t*) (buffer + sizeof(int) + path_length));
                break;
            case CREATE_DIR:
                //create_dir(id, SimpleString(buffer + sizeof(int), path_length), *(mode_t*) (buffer + sizeof(int) + path_length));
                std::thread(&Server::create_dir, this, id, SimpleString(buffer + sizeof(int), path_length), *(mode_t*) (buffer + sizeof(int) + path_length));
                break;
            case GET_FILE_ATTR:
                //get_file_attr(id, SimpleString(buffer + sizeof(int), path_length));
                std::thread(&Server::get_file_attr, this, id, SimpleString(buffer + sizeof(int), path_length));
                break;
            //TODO
            default:
                LOG("Unknown request type %d", type);
                disconnect();
                return;
        }
    }
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

void Server::create_file(int id, SimpleString path, mode_t mode) {
    
    cout << "create_file" << endl;
    cout << "path: " << string(path.data, path.length) << endl;
    cout << "mode: " << mode << endl;

    assert(engine != NULL);

    int status = engine->create_file(path, mode);

    response(id, status, 0, 0, 0, NULL, 0, NULL);
}

void Server::create_dir(int id, SimpleString path, mode_t mode) {
    
    cout << "create_dir" << endl;
    cout << "path: " << string(path.data, path.length) << endl;
    cout << "mode: " << mode << endl;

    assert(engine != NULL);

    int status = engine->create_dir(path, mode);

    response(id, status, 0, 0, 0, NULL, 0, NULL);
}

void Server::get_file_attr(int id, SimpleString path) {
        
    cout << "get_file_attr" << endl;
    cout << "path: " << string(path.data, path.length) << endl;

    assert(engine != NULL);

    struct stat attr;
    
    int status = engine->get_file_attr(path, &attr);

    response(id, status, 0, sizeof(struct stat), sizeof(struct stat), &attr, 0, NULL);
}