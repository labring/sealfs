/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPLv2.
  See the file COPYING.
*/

/** @file
 *
 * minimal example filesystem using high-level API
 *
 * Compile with:
 *
 *     gcc -Wall hello.c `pkg-config fuse3 --cflags --libs` -o hello
 *
 * ## Source code ##
 * \include hello.c
 */

#include <client/client.hpp>
#include <client/logging.hpp>
#include <assert.h>

/* get global client instance, thread-safe */

Node::Node(char* host, char* port) {
	this->host = host;
	this->port = port;
}

Node::~Node() {
	delete this->host;
	delete this->port;
	if (this->connection != NULL) {
		this->connection->disconnect();
		delete this->connection;
	}
}

inline Connection* Node::get_connection() {
	if (this->connection == NULL) {
		this->connection = new Connection(this->host, this->port);
	}
	if (this->connection->sock == -1) {
		if (this->connection->reconnect() < 0) {
			LOG("Failed to reconnect to %s:%s", this->host, this->port);
			return NULL;
		}
	}
	return this->connection;
}

inline void Node::disconnect() {
	if (this->connection != NULL) {
		this->connection->disconnect();
	}
}

Client* get_client() {
	if (!client) {
		client_write_lock.lock();
		if (!client) {
			client = new Client();
		}
		client_write_lock.unlock();
	}
	return client; 
}

Client::Client() {}

Client::~Client() {}

inline Connection* Client::get_connection(int index) {
	auto iter_pair = server_list.equal_range(index);
	if (iter_pair.first == server_list.end()) {
		return server_list.begin()->second->get_connection();
	}
	return iter_pair.first->second->get_connection();
}

int Client::delete_connection(int index) {
	//TODO
	return 0;
}

int Client::add_server(const char* host, const char* port) {
	assert(server_list.size() == 0);
	int index = 0;
	Node* node = new Node((char*)host, (char*)port);
	server_list.insert(std::pair<int, Node*>(index, node));
	return index;
}

int Client::map_path(const char* path) {
	return 0;
}


int Client::create_remote_file(const char *path, mode_t mode) {
	Connection* conn = get_connection(map_path(path));
	if (conn == NULL) {
		return -EIO;
	}
	return conn->create_remote_file(path, mode);
}

int Client::create_remote_dir(const char *path, mode_t mode) {
	Connection* conn = get_connection(map_path(path));
	if (conn == NULL) {
		return -EIO;
	}
	return conn->create_remote_dir(path, mode);
}

int Client::get_remote_file_attr(const char *path, struct stat *stbuf) {
	Connection* conn = get_connection(map_path(path));
	if (conn == NULL) {
		return -EIO;
	}
	return conn->get_remote_file_attr(path, stbuf);
}

int Client::read_remote_dir(const char *path, void *buf, fuse_fill_dir_t filler) {
	Connection* conn = get_connection(map_path(path));
	if (conn == NULL) {
		return -EIO;
	}
	return conn->read_remote_dir(path, buf, filler);
}

int Client::open_remote_file(const char *path, struct fuse_file_info *fi) {
	Connection* conn = get_connection(map_path(path));
	if (conn == NULL) {
		return -EIO;
	}
	return conn->open_remote_file(path, fi);
}

int Client::read_remote_file(const char *path, char *buf, size_t size, off_t offset) {
	Connection* conn = get_connection(map_path(path));
	if (conn == NULL) {
		return -EIO;
	}
	return conn->read_remote_file(path, buf, size, offset);
}

int Client::write_remote_file(const char *path, const char *buf, size_t size, off_t offset) {
	Connection* conn = get_connection(map_path(path));
	if (conn == NULL) {
		return -EIO;
	}
	return conn->write_remote_file(path, buf, size, offset);
}
