
#include <deamon/server.hpp>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <vector>
#include <thread>
#include <iostream>

int main() {
    std::cout << "Hello World!" << std::endl;
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        std::cerr << "Could not create socket" << std::endl;
    }
    std::cout << "Socket created" << std::endl;

    struct sockaddr_in server;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_family = AF_INET;
    server.sin_port = htons(8888);

    if (bind(sock, (struct sockaddr*)&server, sizeof(server)) < 0) {
        std::cerr << "bind failed. Error" << std::endl;
        return 1;
    }
    std::cout << "bind done" << std::endl;

    if (listen(sock, 3) == -1) {
        std::cerr << "listen failed. Error" << std::endl;
        return 1;
    }

    std::vector<Server*> servers;
    std::vector<std::thread*> threads;

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);

    while (1) {
        struct sockaddr_in client;
        int c = sizeof(struct sockaddr_in);
        std::cout << "Waiting for incoming connections..." << std::endl;
        int new_socket = accept(sock, (struct sockaddr*)&client, (socklen_t*)&c);
        if (new_socket < 0) {
            std::cerr << "accept failed" << std::endl;
            return 1;
        }
        std::cout << "Connection accepted" << std::endl;

        Server* server = new Server(new_socket, client, db);
        servers.push_back(server);
        std::thread* thread = new std::thread(&Server::parse_request, server);
        threads.push_back(thread);
        std::cout << "Handler assigned" << std::endl;
    }
}