#ifndef SERVER_UTIL_HPP
#define SERVER_UTIL_HPP

#include <string>
#include <leveldb/db.h>

inline std::string add_dir(std::string dirs, std::string dir) {
    return dirs + (char)(u_int8_t)dir.size() + dir;
}

/* grenerate new file name by path */

inline std::string grenerate_local_file_name(leveldb::Slice path) {

    /* mock: return two random string of 16-bit, split by '/' */
    
    std::string file_name;
    for (int i = 0; i < 16; i++) {
        file_name += (char)('a' + rand() % 26);
    }
    file_name += '/';
    for (int i = 0; i < 16; i++) {
        file_name += (char)('a' + rand() % 26);
    }
    return file_name;
}

#endif /* SERVER_UTIL_HPP */