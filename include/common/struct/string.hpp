#ifndef STRING_STRUCT_HPP
#define STRING_STRUCT_HPP

struct SimpleString {
    const char* data;
    int length;
    SimpleString(const char* data, int length) : data(data), length(length) {}
    SimpleString() : data(NULL), length(0) {}
};

#endif /* STRING_STRUCT_HPP */