#include <stdio.h>
#include <stdlib.h>
#include "stomp.h"

int foo_cb(const char *message)
{
    printf("Message: '%s'\n", message);
    return 1;
}
int main(int argc, char **argv)
{
    void *handle = stomp_create_consumer(argv[1], atoi(argv[2]), argv[3], foo_cb, atoi(argv[4]));
    //usleep(3 * 1000 * 1000);
    stomp_relase(handle);
    return 0;
}
