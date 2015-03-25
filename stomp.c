#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <errno.h>
#include <pthread.h>

#define MAX_EVENTS (64)
#define BUFFER_SIZE (10 * 1024 * 1024)
#define CONNECTED "CONNECTED"
#define ERROR "ERROR"
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
    int                sockfd, epfd, nfds;
    struct sockaddr_in sock_addr;
    struct epoll_event ev = {0}, *events;
    int                length = 0, buffer_size = BUFFER_SIZE;
    printf("worker_thread_proc up\n");
    if ((epfd = epoll_create1(0)) == -1)
    {
        printf("epoll_create1() failed - [errno: %d - %s]\n",
               errno,
               strerror(errno));
        exit(EXIT_FAILURE);
    }
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
        close(epfd);
        free(ibuf);
        free(obuf);
        exit(EXIT_FAILURE);
    }
    if ((sockfd = socket(AF_INET , SOCK_STREAM , 0)) == -1)
    {
        printf("socket() failed - [errno: %d - %s]\n",
               errno,
               strerror(errno));
        close(epfd);
        free(ibuf);
        free(obuf);
        exit(EXIT_FAILURE);
    }
    set_non_blocking(sockfd);
    ev.data.fd = sockfd;
    // EPOLLET: Edge Triggered
    // EPOLLRDHUP: Stream socket peer closed connection, or shut down writing half of connection.
    // EPOLLIN: The associated file is available for read operations
    // EPOLLOUT: The associated file is available for write operations.
    ev.events = EPOLLET | EPOLLRDHUP | EPOLLIN | EPOLLOUT;
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, sockfd, &ev) == -1)
    {
        printf("epoll_ctl(EPOLL_CTL_ADD) failed - [errno: %d - %s]\n",
               errno,
               strerror(errno));
        close(sockfd);
        close(epfd);
        free(ibuf);
        free(obuf);
        exit(EXIT_FAILURE);
    }
    memset(&sock_addr, 0, sizeof(struct sockaddr_in));
    sock_addr.sin_addr.s_addr = paddr->s_addr;
    sock_addr.sin_family = AF_INET;
    sock_addr.sin_port = htons(internal->port);
    connect(sockfd, (struct sockaddr *)&sock_addr, sizeof(struct sockaddr_in));
    events = (struct epoll_event *)calloc(MAX_EVENTS, sizeof(struct epoll_event));
    while (internal->status != STATUS_TCP_DISCONNECT)
    {
        while ((nfds = epoll_wait(epfd, events, MAX_EVENTS, internal->heart_beat << 2)) == -1 && errno == EINTR);
        if (internal->status != STATUS_TCP_DISCONNECT)
        {
            int i;
            if (nfds == -1)
            {
                printf("epoll_wait() - [errno: %d - %s]\n",
                       errno,
                       strerror(errno));
                free(events);
                close(sockfd);
                close(epfd);
                free(ibuf);
                free(obuf);
                exit(EXIT_FAILURE);
            }
            else if (nfds == 0)
            {
                events[0].data.fd = sockfd;
                events[0].events = EPOLLRDHUP;
                nfds = 1;
                //printf("force reset after time out\n");
            }
            for (i = 0; i < nfds; i++)
            {
                if (events[i].events & EPOLLRDHUP)
                {
                    close(events[i].data.fd);
                    internal->status = STATUS_TCP_CONNECT;
                    usleep(internal->heart_beat * 1000);
                    gethostbyname_r(internal->hostname, &hostbuf, ibuf, buffer_size, &host, &err);
                    sockfd = socket(AF_INET , SOCK_STREAM , 0);
                    set_non_blocking(sockfd);
                    ev.data.fd = sockfd;
                    ev.events = EPOLLET | EPOLLRDHUP | EPOLLIN | EPOLLOUT;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, sockfd, &ev);
                    memset(&sock_addr, 0, sizeof(struct sockaddr_in));
                    sock_addr.sin_addr.s_addr = paddr->s_addr;
                    sock_addr.sin_family = AF_INET;
                    sock_addr.sin_port = htons(internal->port);
                    connect(sockfd, (struct sockaddr *)&sock_addr, sizeof(struct sockaddr_in));
                    length = 0;
                }
                else if (events[i].events & EPOLLIN)
                {
                    while (1)
                    {
                        int n = recv(events[i].data.fd, ibuf + length, buffer_size - 1 - length, 0);
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
                                    send_all(events[i].data.fd,
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
                                        send_all(events[i].data.fd,
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
                                        close(events[i].data.fd);
                                        internal->status = STATUS_TCP_CONNECT;
                                        gethostbyname_r(internal->hostname, &hostbuf, ibuf, buffer_size, &host, &err);
                                        sockfd = socket(AF_INET , SOCK_STREAM , 0);
                                        set_non_blocking(sockfd);
                                        ev.data.fd = sockfd;
                                        ev.events = EPOLLET | EPOLLRDHUP | EPOLLIN | EPOLLOUT;
                                        epoll_ctl(epfd, EPOLL_CTL_ADD, sockfd, &ev);
                                        memset(&sock_addr, 0, sizeof(struct sockaddr_in));
                                        sock_addr.sin_addr.s_addr = paddr->s_addr;
                                        sock_addr.sin_family = AF_INET;
                                        sock_addr.sin_port = htons(internal->port);
                                        connect(sockfd, (struct sockaddr *)&sock_addr, sizeof(struct sockaddr_in));
                                        length = 0;
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
                                    break;
                                }
                            }
                            for (i = 0; i < length; i++)
                            {
                                if (ibuf[i] != '\n')
                                {
                                    break;
                                }
                            }
                            if (i > 0 && (length -= i) > 0)
                            {
                                memmove(ibuf, ibuf + i, length);
                            }
                        }
                        else if (errno == EAGAIN || errno == EWOULDBLOCK)
                        {
                            break;
                        }
                    }
                }
                else if (events[i].events & EPOLLOUT)
                {
                    if (internal->status == STATUS_TCP_CONNECT)
                    {
                        internal->status = STATUS_STOMP_CONNECT;
                        send_all(events[i].data.fd,
                                 obuf,
                                 sprintf(obuf,
                                         "STOMP\naccept-version:1.1\nreceipt:stomp-%d\nheart-beat:0,%u\n\n%c\n",
                                         internal->receipt++,
                                         internal->heart_beat,
                                         '\0'),
                                 MSG_NOSIGNAL);
                        //printf("oFrame: '%s'\n", obuf);
                    }
                }
            }
        }
    }
    free(events);
    close(sockfd);
    close(epfd);
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

