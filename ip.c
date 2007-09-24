/*
 * qperf - handle socket tests.
 *
 * Copyright (c) 2002-2007 Johann George.  All rights reserved.
 * Copyright (c) 2006-2007 QLogic Corporation.  All rights reserved.
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
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "qperf.h"


/*
 * Parameters.
 */
#define AF_INET_SDP 27                  /* Family for SDP */
#define AF_INET_RDS 30                  /* Family for RDS */


/*
 * Function prototypes.
 */
static void     datagram_client_bw(int domain);
static void     datagram_client_init(int *fd, int domain,
                                     struct sockaddr_in *addr);
static void     datagram_client_lat(int domain);
static void     datagram_server_bw(int domain);
static int      datagram_server_init(int *fd, int domain);
static void     datagram_server_lat(int domain);
static uint32_t decode_port(uint32_t *p);
static void     encode_port(uint32_t *p, uint32_t port);
static void     ip_parameters(long msgSize);
static int      recv_full(int fd, void *ptr, int len);
static int      send_full(int fd, void *ptr, int len);
static int      set_socket_buffer_size(int fd);
static void     socket_client_bw(int domain);
static void     socket_client_init(int *fd, int domain);
static void     socket_client_lat(int domain);
static void     socket_server_bw(int domain);
static int      socket_server_init(int *fd, int domain);
static void     socket_server_lat(int domain);


/*
 * Measure RDS bandwidth (client side).
 */
void
run_client_rds_bw(void)
{
    ip_parameters(8*1024);
    datagram_client_bw(AF_INET_RDS);
}


/*
 * Measure RDS bandwidth (server side).
 */
void
run_server_rds_bw(void)
{
    datagram_server_bw(AF_INET_RDS);
}


/*
 * Measure RDS latency (client side).
 */
void
run_client_rds_lat(void)
{
    ip_parameters(1);
    datagram_client_lat(AF_INET_RDS);
}


/*
 * Measure RDS latency (server side).
 */
void
run_server_rds_lat(void)
{
    datagram_server_lat(AF_INET_RDS);
}


/*
 * Measure UDP bandwidth (client side).
 */
void
run_client_udp_bw(void)
{
    ip_parameters(32*1024);
    datagram_client_bw(AF_INET);
}


/*
 * Measure UDP bandwidth (server side).
 */
void
run_server_udp_bw(void)
{
    datagram_server_bw(AF_INET);
}


/*
 * Measure UDP latency (client side).
 */
void
run_client_udp_lat(void)
{
    ip_parameters(1);
    datagram_client_lat(AF_INET);
}


/*
 * Measure UDP latency (server side).
 */
void
run_server_udp_lat(void)
{
    datagram_server_lat(AF_INET);
}


/*
 * Measure SDP bandwidth (client side).
 */
void
run_client_sdp_bw(void)
{
    ip_parameters(64*1024);
    socket_client_bw(AF_INET_SDP);
}


/*
 * Measure SDP bandwidth (server side).
 */
void
run_server_sdp_bw(void)
{
    socket_server_bw(AF_INET_SDP);
}


/*
 * Measure SDP latency (client side).
 */
void
run_client_sdp_lat(void)
{
    ip_parameters(1);
    socket_client_lat(AF_INET_SDP);
}


/*
 * Measure SDP latency (server side).
 */
void
run_server_sdp_lat(void)
{
    socket_server_lat(AF_INET_SDP);
}


/*
 * Measure TCP bandwidth (client side).
 */
void
run_client_tcp_bw(void)
{
    ip_parameters(64*1024);
    socket_client_bw(AF_INET);
}


/*
 * Measure TCP bandwidth (server side).
 */
void
run_server_tcp_bw(void)
{
    socket_server_bw(AF_INET);
}


/*
 * Measure TCP latency (client side).
 */
void
run_client_tcp_lat(void)
{
    ip_parameters(1);
    socket_client_lat(AF_INET);
}


/*
 * Measure TCP latency (server side).
 */
void
run_server_tcp_lat(void)
{
    socket_server_lat(AF_INET);
}


/*
 * Measure socket bandwidth (client side).
 */
static void
socket_client_bw(int domain)
{
    char *buf;
    int sockFD;

    socket_client_init(&sockFD, domain);
    buf = qmalloc(Req.msg_size);
    if (!synchronize())
        goto err;
    while (!Finished) {
        int n = send_full(sockFD, buf, Req.msg_size);
        if (Finished)
            break;
        if (n < 0) {
            LStat.s.no_errs++;
            continue;
        } else {
            LStat.s.no_bytes += n;
            LStat.s.no_msgs++;
        }
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    free(buf);
    close(sockFD);
    show_results(BANDWIDTH);
}


/*
 * Measure socket bandwidth (server side).
 */
static void
socket_server_bw(int domain)
{
    int sockFD;
    char *buf = 0;

    if (!socket_server_init(&sockFD, domain))
        return;
    if (!synchronize())
        goto err;
    buf = qmalloc(Req.msg_size);
    while (!Finished) {
        int n = recv_full(sockFD, buf, Req.msg_size);
        if (Finished)
            break;
        if (n < 0) {
            LStat.r.no_errs++;
            continue;
        } else {
            LStat.r.no_bytes += n;
            LStat.r.no_msgs++;
        }
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    free(buf);
    close(sockFD);
}


/*
 * Measure socket latency (client side).
 */
static void
socket_client_lat(int domain)
{
    char *buf;
    int sockFD;

    socket_client_init(&sockFD, domain);
    buf = qmalloc(Req.msg_size);
    if (!synchronize())
        goto err;
    while (!Finished) {
        int n = send_full(sockFD, buf, Req.msg_size);
        if (Finished)
            break;
        if (n < 0) {
            LStat.s.no_errs++;
            continue;
        } else {
            LStat.s.no_bytes += n;
            LStat.s.no_msgs++;
        }

        n = recv_full(sockFD, buf, Req.msg_size);
        if (Finished)
            break;
        if (n < 0) {
            LStat.r.no_errs++;
            continue;
        } else {
            LStat.r.no_bytes += n;
            LStat.r.no_msgs++;
        }
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    free(buf);
    close(sockFD);
    show_results(LATENCY);
}


/*
 * Measure socket latency (server side).
 */
static void
socket_server_lat(int domain)
{
    int sockFD;
    char *buf = 0;

    if (!socket_server_init(&sockFD, domain))
        return;
    if (!synchronize())
        goto err;
    buf = qmalloc(Req.msg_size);
    while (!Finished) {
        int n = recv_full(sockFD, buf, Req.msg_size);
        if (Finished)
            break;
        if (n < 0) {
            LStat.r.no_errs++;
            continue;
        } else {
            LStat.r.no_bytes += n;
            LStat.r.no_msgs++;
        }

        n = send_full(sockFD, buf, Req.msg_size);
        if (Finished)
            break;
        if (n < 0) {
            LStat.s.no_errs++;
            continue;
        } else {
            LStat.s.no_bytes += n;
            LStat.s.no_msgs++;
        }
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    free(buf);
    close(sockFD);
}


/*
 * Socket client initialization.
 */
static void
socket_client_init(int *fd, int domain)
{
    uint32_t port;
    struct hostent *host;
    struct sockaddr_in clientAddr;
    struct sockaddr_in serverAddr;
    socklen_t clientLen = sizeof(clientAddr);

    client_send_request();
    *fd = socket(domain, SOCK_STREAM, 0);
    if (*fd < 0)
        syserror_die("socket failed");
    clientAddr.sin_family = AF_INET;
    clientAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    clientAddr.sin_port = htons(0);
    if (bind(*fd, (struct sockaddr *)&clientAddr, clientLen) < 0)
        syserror_die("bind failed");
    if (getsockname(*fd, (struct sockaddr *)&clientAddr, &clientLen) < 0)
        syserror_die("getsockname failed");
    if (!set_socket_buffer_size(*fd))
        die();

    host = gethostbyname(ServerName);
    if (!host)
        error_die("cannot find machine %s", ServerName);
    serverAddr.sin_family = AF_INET;
    if (host->h_length > sizeof(serverAddr.sin_addr))
        error_die("address too large to handle");
    memcpy(&serverAddr.sin_addr.s_addr, host->h_addr, host->h_length);
    if (!recv_mesg(&port, sizeof(port), "port"))
        die();
    port = decode_port(&port);
    debug("sending from %s port %d to %d",
                                domain == AF_INET_SDP ? "SDP" : "TCP",
                                            ntohs(clientAddr.sin_port), port);
    serverAddr.sin_port = htons(port);
    if (connect(*fd, &serverAddr, sizeof(serverAddr)) < 0)
        syserror_die("connect failed");
}


/*
 * Socket server initialization.
 */
static int
socket_server_init(int *fd, int domain)
{
    uint32_t port;
    int listenFD;
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);
    int stat = 0;
    int one = 1;

    listenFD = socket(domain, SOCK_STREAM, 0);
    if (listenFD < 0)
        return syserror("socket failed");
    if (setsockopt(listenFD, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
        return syserror("failed to reuse address on socket");
    memset(&addr, 0, len);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(Req.port);
    if (bind(listenFD, (struct sockaddr *)&addr, len) < 0) {
        syserror("bind failed");
        goto err;
    }
    if (getsockname(listenFD, (struct sockaddr *)&addr, &len) < 0) {
        syserror("getsockname failed");
        goto err;
    }
    port = ntohs(addr.sin_port);
    if (listen(listenFD, 1) < 0) {
        syserror("listen failed");
        goto err;
    }
    encode_port(&port, port);
    if (!send_mesg(&port, sizeof(port), "port"))
        goto err;
    len = sizeof(addr);
    *fd = accept(listenFD, (struct sockaddr *)&addr, &len);
    if (*fd < 0) {
        syserror("accept failed");
        goto err;
    }
    debug("accepted connection");
    if (!set_socket_buffer_size(*fd)) {
        close(*fd);
        goto err;
    }
    stat = 1;
err:
    close(listenFD);
    return stat;
}


/*
 * Set both the send and receive socket buffer sizes.
 */
static int
set_socket_buffer_size(int fd)
{
    int size = Req.sock_buf_size;

    if (!size)
        return 1;
    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) < 0)
        return syserror("failed to set send buffer size on socket");
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size)) < 0)
        return syserror("failed to set receive buffer size on socket");
    return 1;
}


static void
datagram_client_bw(int domain)
{
    char *buf;
    int sockFD;
    struct sockaddr_in serverAddr;

    datagram_client_init(&sockFD, domain, &serverAddr);
    buf = qmalloc(Req.msg_size);
    if (!synchronize())
        goto err;
    while (!Finished) {
        int n = sendto(sockFD, buf, Req.msg_size, 0,
                       (struct sockaddr *)&serverAddr, sizeof(serverAddr));
        if (Finished)
            break;
        if (n < 0) {
            LStat.s.no_errs++;
            continue;
        } else {
            LStat.s.no_bytes += n;
            LStat.s.no_msgs++;
        }
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    free(buf);
    close(sockFD);
    show_results(BANDWIDTH_SR);
}


static void
datagram_server_bw(int domain)
{
    int sockFD;
    char *buf = 0;

    if (!datagram_server_init(&sockFD, domain))
        return;
    if (!synchronize())
        goto err;
    buf = qmalloc(Req.msg_size);
    while (!Finished) {
        int n = recv(sockFD, buf, Req.msg_size, 0);
        if (Finished)
            break;
        if (n < 0) {
            LStat.r.no_errs++;
            continue;
        } else {
            LStat.r.no_bytes += n;
            LStat.r.no_msgs++;
        }
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    free(buf);
    close(sockFD);
}


static void
datagram_client_lat(int domain)
{
    char *buf;
    int sockFD;
    struct sockaddr_in addr;

    datagram_client_init(&sockFD, domain, &addr);
    buf = qmalloc(Req.msg_size);
    if (!synchronize())
        goto err;
    while (!Finished) {
        int n = sendto(sockFD, buf, Req.msg_size, 0,
                       (struct sockaddr *)&addr, sizeof(addr));
        if (Finished)
            break;
        if (n < 0) {
            LStat.s.no_errs++;
            continue;
        } else {
            LStat.s.no_bytes += n;
            LStat.s.no_msgs++;
        }

        n = recv(sockFD, buf, Req.msg_size, 0);
        if (Finished)
            break;
        if (n < 0) {
            LStat.r.no_errs++;
            continue;
        } else {
            LStat.r.no_bytes += n;
            LStat.r.no_msgs++;
        }
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    free(buf);
    close(sockFD);
    show_results(LATENCY);
}


/*
 * Set default IP parameters and ensure that any that are set are being used.
 */
static void
ip_parameters(long msgSize)
{
    setp_u32(0, L_MSG_SIZE, msgSize);
    setp_u32(0, R_MSG_SIZE, msgSize);
    par_use(L_PORT);
    par_use(R_PORT);
    par_use(L_SOCK_BUF_SIZE);
    par_use(R_SOCK_BUF_SIZE);
    opt_check();
}


static void
datagram_server_lat(int domain)
{
    int sockFD;
    char *buf = 0;

    if (!datagram_server_init(&sockFD, domain))
        goto err;
    if (!synchronize())
        goto err;
    buf = qmalloc(Req.msg_size);
    while (!Finished) {
        struct sockaddr_in clientAddr;
        socklen_t clientLen = sizeof(clientAddr);
        int n = recvfrom(sockFD, buf, Req.msg_size, 0,
                         (struct sockaddr *)&clientAddr, &clientLen);
        if (Finished)
            break;
        if (n < 0) {
            LStat.r.no_errs++;
            continue;
        } else {
            LStat.r.no_bytes += n;
            LStat.r.no_msgs++;
        }

        n = sendto(sockFD, buf, Req.msg_size, 0,
                        (struct sockaddr *)&clientAddr, clientLen);
        if (Finished)
            break;
        if (n < 0) {
            LStat.s.no_errs++;
            continue;
        } else {
            LStat.s.no_bytes += n;
            LStat.s.no_msgs++;
        }
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    free(buf);
    close(sockFD);
}


/*
 * Datagram client initialization.
 */
static void
datagram_client_init(int *fd, int domain, struct sockaddr_in *serverAddr)
{
    uint32_t port;
    struct hostent *host;
    struct sockaddr_in clientAddr;
    socklen_t clientLen = sizeof(clientAddr);

    client_send_request();
    *fd = socket(domain, SOCK_DGRAM, 0);
    if (*fd < 0)
        syserror_die("socket failed");
    clientAddr.sin_family = AF_INET;
    clientAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    clientAddr.sin_port = htons(0);
    if (bind(*fd, (struct sockaddr *)&clientAddr, clientLen) < 0)
        syserror_die("bind failed");
    if (getsockname(*fd, (struct sockaddr *)&clientAddr, &clientLen) < 0)
        syserror_die("getsockname failed");
    if (!set_socket_buffer_size(*fd))
        die();

    host = gethostbyname(ServerName);
    if (!host)
        error_die("cannot find machine %s", ServerName);
    serverAddr->sin_family = AF_INET;
    if (host->h_length > sizeof(serverAddr->sin_addr))
        error_die("address too large to handle");
    memcpy(&serverAddr->sin_addr.s_addr, host->h_addr, host->h_length);
    if (!recv_mesg(&port, sizeof(port), "port"))
        die();
    port = decode_port(&port);
    debug("sending from %s port %d to %d",
          domain == AF_INET ? "UDP" : "RDS", ntohs(clientAddr.sin_port), port);
    serverAddr->sin_port = htons(port);
}


/*
 * Datagram server initialization.
 */
static int
datagram_server_init(int *fd, int domain)
{
    uint32_t port;
    struct sockaddr_in addr;
    socklen_t len = sizeof(addr);

    *fd = socket(domain, SOCK_DGRAM, 0);
    if (*fd < 0)
        return syserror("socket failed");
    memset(&addr, 0, len);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(Req.port);
    if (bind(*fd, (struct sockaddr *)&addr, len) < 0) {
        syserror("bind failed");
        goto err;
    }
    if (getsockname(*fd, (struct sockaddr *)&addr, &len) < 0) {
        syserror("getsockname failed");
        goto err;
    }
    if (!set_socket_buffer_size(*fd))
        goto err;
    encode_port(&port, ntohs(addr.sin_port));
    if (!send_mesg(&port, sizeof(port), "port"))
        goto err;
    return 1;

err:
    close(*fd);
    return 0;
}


/*
 * Send a complete message to a socket.  A zero byte write indicates an end of
 * file which suggests that we are finished.
 */
static int
send_full(int fd, void *ptr, int len)
{
    int n = len;
    while (!Finished && n) {
        int i = write(fd, ptr, n);
        if (i < 0)
            return i;
        ptr += i;
        n   -= i;
        if (i == 0)
            set_finished();
    }
    return len-n;
}


/*
 * Receive a complete message from a socket.  A zero byte read indicates an end
 * of file which suggests that we are finished.
 */
static int
recv_full(int fd, void *ptr, int len)
{
    int n = len;
    while (!Finished && n) {
        int i = read(fd, ptr, n);
        if (i < 0)
            return i;
        ptr += i;
        n   -= i;
        if (i == 0)
            set_finished();
    }
    return len-n;
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
