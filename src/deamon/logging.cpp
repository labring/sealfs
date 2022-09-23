#include "common/logging.hpp"

FILE* log_file;
FILE* main_log_file;

/* init logger */

void init_logger(const char* log_file_path) {
    log_file = stdout;
    main_log_file = stderr;
}
