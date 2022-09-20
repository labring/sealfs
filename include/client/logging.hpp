#ifndef CONNECTION_HPP
#define CONNECTION_HPP


#include <stdio.h>
#include <stdlib.h>

extern FILE* log_file;

#define LOG(...) fprintf(log_file, __VA_ARGS__); fprintf(log_file,"\n"); fflush(log_file);

void init_logger(const char* log_file_name);

#endif /* CONNECTION_HPP */