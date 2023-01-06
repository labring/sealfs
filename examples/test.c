#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
int main() {
    int fd = open("/mnt/sealfs/test", O_RDWR);
    printf("%d %s\n", fd, strerror(errno));
    return 0;
}