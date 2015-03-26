#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>
#include <pthread.h>

#define BUFFER_SIZE (10 * 1024 * 1024)
#define CONNECTED "CONNECTED"
#define MESSAGE "MESSAGE"
enum status
{
    STATUS_TCP_DISCONNECT  = 0,
    STATUS_TCP_CONNECT     = 1,
    STATUS_STOMP_CONNECT   = 2,
    STATUS_STOMP_SUBSCRIBE = 3
};
struct stomp_internal_struct
{
    char           *hostname;
    unsigned short  port;
    char           *queuename;
    int           (*message_handler_cb)(const char *message);
    unsigned int    heart_beat;
    pthread_t       threadid;
    int             status;
    int             receipt;
};
int send_all(int         sockfd,
             const char *buf,
             size_t      len,
             int         flags)
{
    size_t  offset = 0;
    ssize_t n = 0;
    do
    {
        n = send(sockfd,
                 buf + offset,
                 len - offset,
                 flags);
        if (n > 0)
        {
            offset += n;
        }
    }
    while (offset < len && n > 0);
    return (offset == len) ? len : -1;
}
void set_non_blocking(int fd)
{
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
}
void *worker_thread_proc(void *handle)
{
    struct stomp_internal_struct *internal = (struct stomp_internal_struct *)handle;
    struct hostent     hostbuf, *host;
    struct in_addr    *paddr;
    char              *ibuf, *obuf;
    int                err;
    int                sockfd, nfds;
    struct sockaddr_in sock_addr;
    int                length, buffer_size = BUFFER_SIZE;
    fd_set             read_fd_set, write_fd_set;
    printf("worker_thread_proc up\n");
    obuf = (char *)calloc(BUFFER_SIZE, sizeof(char));
    ibuf = (char *)calloc(BUFFER_SIZE, sizeof(char));
    gethostbyname_r(internal->hostname, &hostbuf, ibuf, buffer_size, &host, &err);
    if (host == NULL ||
        host->h_addr_list == NULL ||
        (paddr = (struct in_addr *)host->h_addr_list[0]) == NULL)
    {
        printf("gethostbyname_r(%s) failed - [errno: %d - %s]\n",
               internal->hostname,
               errno,
               strerror(errno));
        free(ibuf);
        free(obuf);
        exit(EXIT_FAILURE);
    }
    if ((sockfd = socket(AF_INET , SOCK_STREAM , 0)) == -1)
    {
        printf("socket() failed - [errno: %d - %s]\n",
               errno,
               strerror(errno));
        free(ibuf);
        free(obuf);
        exit(EXIT_FAILURE);
    }
    memset(&sock_addr, 0, sizeof(struct sockaddr_in));
    sock_addr.sin_addr.s_addr = paddr->s_addr;
    sock_addr.sin_family = AF_INET;
    sock_addr.sin_port = htons(internal->port);
    set_non_blocking(sockfd);
    connect(sockfd, (struct sockaddr *)&sock_addr, sizeof(struct sockaddr_in));
    length = 0;
    FD_ZERO(&read_fd_set);
    FD_ZERO(&write_fd_set);
    FD_SET(sockfd, &write_fd_set);
    while (internal->status != STATUS_TCP_DISCONNECT)
    {
        struct timeval tv;
        tv.tv_sec  = internal->heart_beat >> 8;
        tv.tv_usec = (internal->heart_beat & 0xff) << 12;
        //printf("tv.tv_sec = %lu, tv.tv_usec = %lu\n", tv.tv_sec, tv.tv_usec);
        nfds = select(sockfd + 1, &read_fd_set, &write_fd_set, NULL, &tv);
        if (nfds == -1)
        {
            perror ("select");
            exit (EXIT_FAILURE);
        }
        if (internal->status != STATUS_TCP_DISCONNECT)
        {
            if (nfds == 0)
            {
                //printf("force reset after time out\n");
                close(sockfd);
                internal->status = STATUS_TCP_CONNECT;
                gethostbyname_r(internal->hostname, &hostbuf, ibuf, buffer_size, &host, &err);
                sockfd = socket(AF_INET , SOCK_STREAM , 0);
                memset(&sock_addr, 0, sizeof(struct sockaddr_in));
                sock_addr.sin_addr.s_addr = paddr->s_addr;
                sock_addr.sin_family = AF_INET;
                sock_addr.sin_port = htons(internal->port);
                set_non_blocking(sockfd);
                connect(sockfd, (struct sockaddr *)&sock_addr, sizeof(struct sockaddr_in));
                length = 0;
                FD_ZERO(&read_fd_set);
                FD_SET(sockfd, &write_fd_set);
            }
            else
            {
                if (FD_ISSET(sockfd, &read_fd_set))
                {
                    //printf("FD_ISSET(sockfd, &read_fd_set)\n");
                    while (1)
                    {
                        int n = recv(sockfd, ibuf + length, buffer_size - 1 - length, 0);
                        if (n > 0)
                        {
                            int   i;
                            char *pch;
                            length += n;
                            for (i = 0; i < length; i++)
                            {
                                if (ibuf[i] != '\n')
                                {
                                    break;
                                }
                            }
                            if (i > 0)
                            {
                                //printf("heart-beat\n");
                                if ((length -= i) > 0)
                                {
                                    memmove(ibuf, ibuf + i, length);
                                }
                            }
                            while ((pch = (char *) memchr(ibuf, '\0', length)) != NULL) // end with '\0' + '\n'
                            {
                                //printf("iFrame: '%s'\n", ibuf);
                                size_t frame_length = (pch + 2) - ibuf;
                                if (strncmp(ibuf, CONNECTED, sizeof(CONNECTED) - 1) == 0)
                                {
                                    internal->status = STATUS_STOMP_SUBSCRIBE;
                                    send_all(sockfd,
                                             obuf,
                                             sprintf(obuf,
                                                     "SUBSCRIBE\n"\
                                                     "id:0\n"\
                                                     "destination:/queue/%s\n"\
                                                     "ack:client\n"\
                                                     "activemq.prefetchSize:10\n"\
                                                     "\n"\
                                                     "%c\n",
                                                     internal->queuename,
                                                     '\0') + 1,
                                             0);
                                    //printf("oFrame: '%s'\n", obuf);
                                }
                                else if (strncmp(ibuf, MESSAGE, sizeof(MESSAGE) - 1) == 0)
                                {
                                    char *message_id = strstr(ibuf, "message-id:") + 11;
                                    char *ptr = strstr(message_id, "\n");
                                    *ptr = '\0';
                                    ptr = strstr(ptr + 1, "\n\n") + 2;
                                    if (internal->message_handler_cb(ptr))
                                    {
                                        send_all(sockfd,
                                                 obuf,
                                                 sprintf(obuf,
                                                         "ACK\n"\
                                                         "message-id:%s\n"\
                                                         "subscription:0\n"\
                                                         "\n"\
                                                         "%c\n",
                                                         message_id,
                                                         '\0') + 1,
                                                 MSG_NOSIGNAL);
                                        //printf("oFrame: '%s'\n", obuf);
                                    }
                                    else
                                    {
                                        close(sockfd);
                                        internal->status = STATUS_TCP_CONNECT;
                                        gethostbyname_r(internal->hostname, &hostbuf, ibuf, buffer_size, &host, &err);
                                        sockfd = socket(AF_INET , SOCK_STREAM , 0);
                                        memset(&sock_addr, 0, sizeof(struct sockaddr_in));
                                        sock_addr.sin_addr.s_addr = paddr->s_addr;
                                        sock_addr.sin_family = AF_INET;
                                        sock_addr.sin_port = htons(internal->port);
                                        set_non_blocking(sockfd);
                                        connect(sockfd, (struct sockaddr *)&sock_addr, sizeof(struct sockaddr_in));
                                        length = 0;
                                        FD_ZERO(&read_fd_set);
                                        FD_SET(sockfd, &write_fd_set);
                                        break;
                                    }
                                }
                                length -= frame_length;
                                if (length > 0)
                                {
                                    memmove(ibuf, (pch + 2), length);
                                }
                                else
                                {
                                    FD_SET(sockfd, &read_fd_set);
                                    FD_ZERO(&write_fd_set);
                                    break;
                                }
                            }
                            for (i = 0; i < length; i++)
                            {
                                if (ibuf[i] != '\n')
                                {
                                    FD_SET(sockfd, &read_fd_set);
                                    FD_ZERO(&write_fd_set);
                                    break;
                                }
                            }
                            if (i > 0 && (length -= i) > 0)
                            {
                                memmove(ibuf, ibuf + i, length);
                            }
                        }
                        else if (n == 0)
                        {
                            //printf("socket closed - reconnect\n");
                            close(sockfd);
                            internal->status = STATUS_TCP_CONNECT;
                            usleep(internal->heart_beat * 1000);
                            gethostbyname_r(internal->hostname, &hostbuf, ibuf, buffer_size, &host, &err);
                            sockfd = socket(AF_INET , SOCK_STREAM , 0);
                            memset(&sock_addr, 0, sizeof(struct sockaddr_in));
                            sock_addr.sin_addr.s_addr = paddr->s_addr;
                            sock_addr.sin_family = AF_INET;
                            sock_addr.sin_port = htons(internal->port);
                            set_non_blocking(sockfd);
                            connect(sockfd, (struct sockaddr *)&sock_addr, sizeof(struct sockaddr_in));
                            length = 0;
                            FD_ZERO(&read_fd_set);
                            FD_SET(sockfd, &write_fd_set);
                            break;
                        }
                        else if (errno == EAGAIN || errno == EWOULDBLOCK)
                        {
                            FD_SET(sockfd, &read_fd_set);
                            FD_ZERO(&write_fd_set);
                            break;
                        }
                    }
                }
                if (FD_ISSET(sockfd, &write_fd_set))
                {
                    //printf("FD_ISSET(sockfd, &write_fd_set)\n");
                    if (internal->status == STATUS_TCP_CONNECT)
                    {
                        internal->status = STATUS_STOMP_CONNECT;
                        send_all(sockfd,
                                 obuf,
                                 sprintf(obuf,
                                         "STOMP\naccept-version:1.1\nreceipt:stomp-%d\nheart-beat:0,%u\n\n%c\n",
                                         internal->receipt++,
                                         internal->heart_beat,
                                         '\0'),
                                 MSG_NOSIGNAL);
                        //printf("oFrame: '%s'\n", obuf);
                    }
                    FD_SET(sockfd, &read_fd_set);
                    FD_ZERO(&write_fd_set);
                }
            }
        }
    }
    close(sockfd);
    free(ibuf);
    free(obuf);
    printf("worker_thread_proc down\n");
    pthread_exit(0);
    return NULL;
}
void *stomp_create_consumer(const char     *hostname,
                            unsigned short  port,
                            const char     *queuename,
                            int           (*message_handler_cb)(const char *message),
                            unsigned int    heart_beat)
{
    struct stomp_internal_struct *internal = (struct stomp_internal_struct *) malloc(sizeof(struct stomp_internal_struct));
    internal->hostname           = strdup(hostname);
    internal->port               = port;
    internal->queuename          = strdup(queuename);
    internal->message_handler_cb = message_handler_cb;
    internal->heart_beat         = heart_beat;
    internal->receipt            = 0;
    internal->status             = STATUS_TCP_CONNECT;
    pthread_create(&(internal->threadid), NULL, worker_thread_proc, internal);
    return internal;
}
void stomp_relase(void *handle)
{
    struct stomp_internal_struct *internal = (struct stomp_internal_struct *) handle;
    internal->status = STATUS_TCP_DISCONNECT;
    pthread_join(internal->threadid, NULL);
    free(internal->queuename);
    free(internal->hostname);
    free(internal);
}

