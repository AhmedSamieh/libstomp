void *stomp_create_consumer(const char     *hostname,
                            unsigned short  port,
                            const char     *queuename,
                            int           (*message_handler_cb)(const char *message),
                            unsigned int    heart_beat);
void stomp_relase(void *handle);
