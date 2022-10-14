#ifndef CLUSTER_INFO_HPP
#define CLUSTER_INFO_HPP

#include <string>
#include <vector>

extern std::vector<std::pair<std::string, std::string>> cluster_info;

void init_cluster_info(std::string config_server_host, std::string config_server_port);

std::vector<std::pair<std::string, std::string>>& get_servers();

#endif /* CLUSTER_INFO_HPP */