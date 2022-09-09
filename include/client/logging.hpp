
#include <stdio.h>
#include <stdlib.h>

FILE* log_file;

#define LOG(...) fprintf(log_file, __VA_ARGS__)

void init_logger(const char* log_file_name);