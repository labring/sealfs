#ifndef STRING_STRUCT_HPP
#define STRING_STRUCT_HPP

struct SimpleString {
    char* data;
    int length;
    SimpleString(char* data, int length) : data(data), length(length) {}
};

#endif /* STRING_STRUCT_HPP */