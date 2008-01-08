/*
 * qperf - handle RDS tests.
 *
 * Copyright (c) 2002-2008 Johann George.  All rights reserved.
 * Copyright (c) 2006-2008 QLogic Corporation.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include "qperf.h"


/*
 * Parameters.
 */
#define AF_INET_RDS 28                  /* Family for RDS */


/*
 * Function prototypes.
 */
static void     client_get_hosts(void);
static void     client_init(int *fd);
static void     connect_tcp(char *server, char *port, SS *addr,
                                                    socklen_t *len, int *fd);
static uint32_t decode_port(uint32_t *p);
static void     encode_port(uint32_t *p, uint32_t port);
static void     get_sock_ip(SA *saptr, int salen, char *ip, int n);
static void     get_socket_port(int fd, uint32_t *port);
static void     getaddrinfo_rds(int serverflag,
                                            int port, struct addrinfo **aipp);
static void     set_parameters(long msgSize);
static void     server_get_host(void);
static void     server_init(int *fd);
static void     set_socket_buffer_size(int fd);


/*
 * Static variables.
 */
char LHost[NI_MAXHOST];
char RHost[NI_MAXHOST];


/*
 * Measure RDS bandwidth (client side).
 */
void
run_client_rds_bw(void)
{
    char *buf;
    int sockFD;

    set_parameters(8*1024);
    client_init(&sockFD);
    buf = qmalloc(Req.msg_size);
    sync_test();
    while (!Finished) {
        int n = write(sockFD, buf, Req.msg_size);
        if (Finished)
            break;
        if (n != Req.msg_size) {
            LStat.s.no_errs++;
            continue;
        }
        LStat.s.no_bytes += n;
        LStat.s.no_msgs++;
    }
    stop_test_timer();
    exchange_results();
    free(buf);
    close(sockFD);
    show_results(BANDWIDTH_SR);
}


/*
 * Measure RDS bandwidth (server side).
 */
void
run_server_rds_bw(void)
{
    int sockFD;
    char *buf = 0;

    server_init(&sockFD);
    sync_test();
    buf = qmalloc(Req.msg_size);
    while (!Finished) {
        int n = recv(sockFD, buf, Req.msg_size, 0);
        if (Finished)
            break;
        if (n != Req.msg_size) {
            LStat.r.no_errs++;
            continue;
        }
        LStat.r.no_bytes += n;
        LStat.r.no_msgs++;
    }
    stop_test_timer();
    exchange_results();
    free(buf);
    close(sockFD);
}


/*
 * Measure RDS latency (client side).
 */
void
run_client_rds_lat(void)
{
    char *buf;
    int sockFD;

    set_parameters(1);
    client_init(&sockFD);
    buf = qmalloc(Req.msg_size);
    sync_test();
    while (!Finished) {
        int n = write(sockFD, buf, Req.msg_size);
        if (Finished)
            break;
        if (n != Req.msg_size) {
            LStat.s.no_errs++;
            continue;
        }
        LStat.s.no_bytes += n;
        LStat.s.no_msgs++;

        n = read(sockFD, buf, Req.msg_size);
        if (Finished)
            break;
        if (n != Req.msg_size) {
            LStat.r.no_errs++;
            continue;
        }
        LStat.r.no_bytes += n;
        LStat.r.no_msgs++;
    }
    stop_test_timer();
    exchange_results();
    free(buf);
    close(sockFD);
    show_results(LATENCY);
}


/*
 * Measure RDS latency (server side).
 */
void
run_server_rds_lat(void)
{
    char *buf;
    int sockfd;

    server_init(&sockfd);
    sync_test();
    buf = qmalloc(Req.msg_size);
    while (!Finished) {
        struct sockaddr_storage clientAddr;
        socklen_t clientLen = sizeof(clientAddr);
        int n = recvfrom(sockfd, buf, Req.msg_size, 0,
                         (SA *)&clientAddr, &clientLen);
        if (Finished)
            break;
        if (n != Req.msg_size) {
            LStat.r.no_errs++;
            continue;
        }
        LStat.r.no_bytes += n;
        LStat.r.no_msgs++;

        n = sendto(sockfd, buf, Req.msg_size, 0, (SA *)&clientAddr, clientLen);
        if (Finished)
            break;
        if (n != Req.msg_size) {
            LStat.s.no_errs++;
            continue;
        }
        LStat.s.no_bytes += n;
        LStat.s.no_msgs++;
    }
    stop_test_timer();
    exchange_results();
    free(buf);
    close(sockfd);
}


/*
 * Set default IP parameters and ensure that any that are set are being used.
 */
static void
set_parameters(long msgSize)
{
    setp_u32(0, L_MSG_SIZE, msgSize);
    setp_u32(0, R_MSG_SIZE, msgSize);
    par_use(L_PORT);
    par_use(R_PORT);
    par_use(L_SOCK_BUF_SIZE);
    par_use(R_SOCK_BUF_SIZE);
    opt_check();
}


/*
 * Socket client initialization.
 */
static void
client_init(int *fd)
{
    uint32_t rport;
    struct addrinfo *ai, *ailist;

    client_send_request();
    client_get_hosts();
    recv_mesg(&rport, sizeof(rport), "port");
    rport = decode_port(&rport);
    getaddrinfo_rds(0, rport, &ailist);
    for (ai = ailist; ai; ai = ai->ai_next) {
        if (!ai->ai_family)
            continue;
        *fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (connect(*fd, ai->ai_addr, ai->ai_addrlen) == SUCCESS0)
            break;
        close(*fd);
    }
    freeaddrinfo(ailist);
    if (!ai)
        error(0, "could not make RDS connection to server");
    if (Debug) {
        uint32_t lport;
        get_socket_port(*fd, &lport);
        debug("sending from RDS port %d to %d", lport, rport);
    }
}


/*
 * Datagram server initialization.
 */
static void
server_init(int *fd)
{
    uint32_t port;
    struct addrinfo *ai, *ailist;
    int sockfd = -1;

    server_get_host();
    getaddrinfo_rds(1, Req.port, &ailist);
    for (ai = ailist; ai; ai = ai->ai_next) {
        if (!ai->ai_family)
            continue;
        sockfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sockfd < 0)
            continue;
        setsockopt_one(sockfd, SO_REUSEADDR);
        if (bind(sockfd, ai->ai_addr, ai->ai_addrlen) == SUCCESS0)
            break;
        close(sockfd);
    }
    freeaddrinfo(ailist);
    if (!ai)
        error(0, "unable to make RDS socket");

    set_socket_buffer_size(sockfd);
    get_socket_port(sockfd, &port);
    encode_port(&port, port);
    send_mesg(&port, sizeof(port), "port");
    *fd = sockfd;
}


/*
 * A version of getaddrinfo that takes a numeric port and prints out an error
 * on failure.
 */
static void
getaddrinfo_rds(int serverflag, int port, struct addrinfo **aipp)
{
    int stat;
    char *service;
    struct addrinfo *aip;
    struct addrinfo hints ={
        .ai_flags    = AI_NUMERICSERV,
        .ai_family   = AF_UNSPEC,
        .ai_socktype = SOCK_DGRAM
    };

    if (serverflag)
        hints.ai_flags |= AI_PASSIVE;

    service = qasprintf("%d", port);
    stat = getaddrinfo(serverflag ? 0 : ServerName, service, &hints, aipp);
    free(service);
    if (stat != SUCCESS0)
        error(0, "getaddrinfo failed: %s", gai_strerror(stat));
    for (aip = *aipp; aip; aip = aip->ai_next) {
        if (aip->ai_socktype == SOCK_DGRAM && aip->ai_family == AF_INET) {
            aip->ai_family = AF_INET_RDS;
            aip->ai_socktype = SOCK_SEQPACKET;
        } else
            aip->ai_family = 0;
    }
}


/*
 * Have an exchange with the client over TCP/IP and get the IP of our local
 * host.
 */
static void
server_get_host(void)
{
    int fd, lfd;
    uint32_t port;
    char rhost[NI_MAXHOST];
    struct sockaddr_in laddr, raddr;
    socklen_t rlen;

    lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0)
        error(SYS, "socket failed");
    setsockopt_one(lfd, SO_REUSEADDR);

    memset(&laddr, 0, sizeof(laddr));
    laddr.sin_family = AF_INET;
    laddr.sin_addr.s_addr = INADDR_ANY;
    laddr.sin_port = htons(0);
    if (bind(lfd, (SA *)&laddr, sizeof(laddr)) < 0)
        error(SYS, "bind failed");

    get_socket_port(lfd, &port);
    encode_port(&port, port);
    send_mesg(&port, sizeof(port), "port");

    if (listen(lfd, 1) < 0)
        error(SYS, "listen failed");

    rlen = sizeof(raddr);
    fd = accept(lfd, (SA *)&raddr, &rlen);
    if (fd < 0)
        error(SYS, "accept failed");
    close(lfd);
    get_sock_ip((SA *)&raddr, rlen, rhost, sizeof(rhost));
    send_mesg(rhost, NI_MAXHOST, "IP");
    recv_mesg(LHost, NI_MAXHOST, "IP");
    close(fd);
}


/*
 * Have an exchange with the server over TCP/IP and get the IPs of our local
 * and the remote host.
 */
static void
client_get_hosts(void)
{
    SS raddr;
    socklen_t rlen;
    char *service;
    uint32_t port;
    int fd = -1;

    recv_mesg(&port, sizeof(port), "port");
    port = decode_port(&port);
    service = qasprintf("%d", port);
    connect_tcp(ServerName, service, &raddr, &rlen, &fd);
    free(service);
    get_sock_ip((SA *)&raddr, rlen, RHost, NI_MAXHOST);
    send_mesg(RHost, NI_MAXHOST, "IP");
    recv_mesg(LHost, NI_MAXHOST, "IP");
    close(fd);
}


/*
 * Connect over TCP/IP to the server/port and return the socket structure, its
 * length and the open socket file descriptor.
 */
static void
connect_tcp(char *server, char *port, SS *addr, socklen_t *len, int *fd)
{
    int stat;
    struct addrinfo *aip, *ailist;
    struct addrinfo hints ={
        .ai_flags    = AI_NUMERICSERV,
        .ai_family   = AF_INET,
        .ai_socktype = SOCK_STREAM
    };

    stat = getaddrinfo(server, port, &hints, &ailist);
    if (stat != 0)
        error(0, "getaddrinfo failed: %s", gai_strerror(stat));
    for (aip = ailist; aip; aip = aip->ai_next) {
        if (fd) {
            *fd = socket(aip->ai_family, aip->ai_socktype, aip->ai_protocol);
            if (*fd < 0)
                error(SYS, "socket failed");
            if (connect(*fd, aip->ai_addr, aip->ai_addrlen) < 0)
                error(SYS, "connect failed");
            break;
        }
        break;
    }
    if (!aip)
        error(0, "connect_tcp failed");
    memcpy(addr, aip->ai_addr, aip->ai_addrlen);
    *len = aip->ai_addrlen;
    freeaddrinfo(ailist);
}


/*
 * Given an open socket, return the port associated with it.  There must be a
 * more efficient way to do this that is portable.
 */
static void
get_socket_port(int fd, uint32_t *port)
{
    char p[NI_MAXSERV];
    struct sockaddr_storage sa;
    socklen_t salen = sizeof(sa);

    if (getsockname(fd, (SA *)&sa, &salen) < 0)
        error(SYS, "getsockname failed");
    if (getnameinfo((SA *)&sa, salen, 0, 0, p, sizeof(p), NI_NUMERICSERV) < 0)
        error(SYS, "getnameinfo failed");
    *port = atoi(p);
    if (!port)
        error(SYS, "invalid port");
}


/*
 * Given a socket, return its IP address.
 */
static void
get_sock_ip(SA *saptr, int salen, char *ip, int n)
{
    int stat = getnameinfo(saptr, salen, ip, n, 0, 0, NI_NUMERICHOST);
    if (stat < 0)
        error(0, "getnameinfo failed: %s", gai_strerror(stat));
}


/*
 * Set both the send and receive socket buffer sizes.
 */
static void
set_socket_buffer_size(int fd)
{
    int size = Req.sock_buf_size;

    if (!size)
        return;
    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) < 0)
        error(SYS, "failed to set send buffer size on socket");
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size)) < 0)
        error(SYS, "failed to set receive buffer size on socket");
}


/*
 * Encode a port which is stored as a 32 bit unsigned.
 */
static void
encode_port(uint32_t *p, uint32_t port)
{
    enc_init(p);
    enc_int(port, sizeof(port));
}


/*
 * Decode a port which is stored as a 32 bit unsigned.
 */
static uint32_t
decode_port(uint32_t *p)
{
    dec_init(p);
    return dec_int(sizeof(uint32_t));
}
