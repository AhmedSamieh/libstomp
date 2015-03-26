#include <stdio.h>
#include <stdlib.h>
#include "stomp.h"
int ok = 0;
int foo_cb(const char *message)
{
    if ((ok = !ok))
    printf("Message: '%s'\n", message);
    else
    printf("Error: '%s'\n", message);
    return ok;
}
int main(int argc, char **argv)
{
    void *handle = stomp_create_consumer(argv[1], atoi(argv[2]), argv[3], foo_cb, atoi(argv[4]));
    usleep(6 * 1000 * 1000);
    stomp_relase(handle);
    return 0;
}
