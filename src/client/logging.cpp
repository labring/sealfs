#include "client/logging.hpp"

/* init logger */

void init_logger(const char* log_file_name) {
    log_file = fopen(log_file_name, "w");
    if (log_file == NULL) {
        perror("fopen");
        exit(1);
    }
}
