
#include <deamon/server.hpp>
#include <deamon/engine.hpp>
#include <common/logging.hpp>
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

void create_server(int m_socket, Engine* m_engine) {
    LOG("Creating server");
    Server* server = new Server(m_socket, m_engine);
    server->parse_request();
    delete server;
}

int main() {

    init_logger("server.log");
    LOG("Starting server");

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        LOG("Could not create socket");
    }
    LOG("Socket created");

    struct sockaddr_in server;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_family = AF_INET;
    server.sin_port = htons(8888);  //mock

    if (bind(sock, (struct sockaddr*)&server, sizeof(server)) < 0) {
        LOG("bind failed. Error");
        return 1;
    }
    LOG("bind done");

    if (listen(sock, 3) == -1) {
        LOG("listen failed. Error");
        return 1;
    }

    std::vector<Server*> servers;
    std::vector<std::thread*> threads;

    Engine engine;

    engine.init();

    while (1) {
        struct sockaddr_in client;
        int c = sizeof(struct sockaddr_in);
        LOG("Waiting for incoming connections...");
        int new_socket = accept(sock, (struct sockaddr*)&client, (socklen_t*)&c);
        if (new_socket < 0) {
            LOG("accept failed");
            return 1;
        }
        LOG("Connection accepted");

        std::thread* thread = new std::thread(create_server, new_socket, &engine);
        threads.push_back(thread);
        LOG("Handler assigned");
    }
}