#ifndef PROTOCOL_HPP
#define PROTOCOL_HPP

#define HEADER_SIZE 16

#define MAX_BUFFER_SIZE 65535


enum OperationType {
    CREATE_FILE = 1,
    CREATE_DIR = 2,
    GET_FILE_ATTR = 3,
    READ_DIR = 4,
    OPEN_FILE = 5,
    READ_FILE = 6,
    WRITE_FILE = 7,
};

enum CallbackState {
    EMPTY = 0,
    IN_PROGRESS = 1,
    DONE = 2,
};

#endif /* PROTOCOL_HPP */