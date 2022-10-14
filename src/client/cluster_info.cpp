#include <client/cluster_info.hpp>

std::vector<std::pair<std::string, std::string>> cluster_info;

void init_cluster_info(std::string config_server_host, std::string config_server_port) {
    cluster_info.push_back(std::make_pair(config_server_host, config_server_port)); // mock
}

std::vector<std::pair<std::string, std::string>>& get_servers() {
    return cluster_info; // mock
}