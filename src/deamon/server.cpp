#include "deamon/server.hpp"
#include <iostream>

using namespace std;


Server::Server(int m_socket) {
    sock = m_socket;
    connected = true;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "./testdb", &db);
    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
    } else {
        std::cout << "success!" << std::endl;
    }
}

Server::~Server() {
    disconnect();
}

void Server::disconnect() {
    if (connected) {
        close(sock);
        connected = false;
    }
}

void Server::parse_request() {
    while (connected) {
        char buf[1024];
        int bytes_read = read(sock, buf, 1024);
        if (bytes_read == 0) {
            disconnect();
            break;
        }
        if (bytes_read == -1) {
            printf("Error reading from socket");
            break;
        }
        printf("%s", buf);
    }
}

int Server::response(const void* buf, int size) {
    // send(sock, (void*)&size, sizeof(int), 0);
    // int bytes_sent = send(sock, buf, size, 0);
    // if (bytes_sent == -1) {
    //     printf("Error sending response");
    //     return -1;
    // }
    return 0;
}

void Server::create_file(const char *path, mode_t mode, uid_t uid) {
    
    cout << "create_file" << endl;
    cout << "path: " << path << endl;
    cout << "mode: " << mode << endl;

    string key(path);
    string *value = NULL;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), key, value);

    if (s.ok()) {
        printf("File already exists");
        int buf[2] = {uid, -EEXIST};
        if (response((const void*)buf, sizeof(buf)) == -1) {
            printf("Error sending response");
        }
    } else if (s.IsNotFound()) {
        printf("File not found");
        leveldb::Status s;
        if (mode & S_IFREG) {
            s = db->Put(leveldb::WriteOptions(), key, "f\006\006\006");
        } else if (mode & S_IFDIR) {
            s = db->Put(leveldb::WriteOptions(), key, "d\006\006\006");
        }
        if (s.ok()) {
            int buf[2] = {uid, 0};
            if (response((const void*)buf, sizeof(buf)) == -1) {
                printf("Error sending response");
            }
        } else {
            printf("Error writing to db");
        }
    } else {
        printf("An error occurred");
    }
}