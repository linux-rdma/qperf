/*
 * qperf - general header file.
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


/*
 * Parameters.
 */
#define STRSIZE 64


/*
 * For convenience and readability.
 */
#define SUCCESS0        0
#define cardof(a)       (sizeof(a)/sizeof(*a))
#define endof(a)        (&a[cardof(a)])
#define streq(a, b)     (strcmp(a, b) == 0)
#define is_client()     (ServerName)

typedef uint64_t CLOCK;


/*
 * Time indices.
 */
typedef enum {
    T_REAL,
    T_USER,
    T_NICE,
    T_KERNEL,
    T_IDLE,
    T_IOWAIT,
    T_IRQ,
    T_SOFTIRQ,
    T_STEAL,
    T_N
} TIME_INDEX;


/*
 * Parameter indices.  P_NULL must be 0.
 */
typedef enum {
    P_NULL,
    L_ACCESS_RECV,
    R_ACCESS_RECV,
    L_AFFINITY,
    R_AFFINITY,
    L_FLIP,
    R_FLIP,
    L_ID,
    R_ID,
    L_MSG_SIZE,
    R_MSG_SIZE,
    L_MTU_SIZE,
    R_MTU_SIZE,
    L_NO_MSGS,
    R_NO_MSGS,
    L_POLL_MODE,
    R_POLL_MODE,
    L_PORT,
    R_PORT,
    L_RATE,
    R_RATE,
    L_RD_ATOMIC,
    R_RD_ATOMIC,
    L_SOCK_BUF_SIZE,
    R_SOCK_BUF_SIZE,
    L_TIME,
    R_TIME,
    L_TIMEOUT,
    R_TIMEOUT,
    P_N
} PAR_INDEX;


/*
 * What we are measuring.
 */
typedef enum {
    LATENCY,
    MSG_RATE,
    BANDWIDTH,
    BANDWIDTH_SR
} MEASURE;


/*
 * Request to the server.  Note that most of these must be of type uint32_t
 * because of the way options are set.  The minor version must be changed if
 * there is change to this data structure.
 */
typedef struct REQ {
    uint16_t    ver_maj;                /* Major version */
    uint16_t    ver_min;                /* Minor version */
    uint16_t    ver_inc;                /* Incremental version */
    uint16_t    req_index;              /* Request index (into Tests) */
    uint32_t    flip;                   /* Flip local/remote node functions */
    uint32_t    access_recv;            /* Access data after receiving */
    uint32_t    affinity;               /* Processor affinity */
    uint32_t    poll_mode;              /* Poll mode */
    uint32_t    port;                   /* Port number requested */
    uint32_t    rd_atomic;              /* Number of pending RDMA or atomics */
    uint32_t    timeout;                /* Timeout for messages */
    uint32_t    msg_size;               /* Message Size */
    uint32_t    mtu_size;               /* MTU Size */
    uint32_t    no_msgs;                /* Number of messages */
    uint32_t    sock_buf_size;          /* Socket buffer size */
    uint32_t    time;                   /* Duration in seconds */
    char        id[STRSIZE];            /* Identifier */
    char        rate[STRSIZE];          /* Rate */
} REQ;


/*
 * Transfer statistics.
 */
typedef struct USTAT {
    uint64_t    no_bytes;               /* Number of bytes transfered */
    uint64_t    no_msgs;                /* Number of messages */
    uint64_t    no_errs;                /* Number of errors */
} USTAT;


/*
 * Statistics.
 */
typedef struct STAT {
    uint32_t    no_cpus;                /* Number of processors */
    uint32_t    no_ticks;               /* Ticks per second */
    uint32_t    max_cqes;               /* Maximum CQ entries */
    CLOCK       time_s[T_N];            /* Start times */
    CLOCK       time_e[T_N];            /* End times */
    USTAT       s;                      /* Send statistics */
    USTAT       r;                      /* Receive statistics */
    USTAT       rem_s;                  /* Remote send statistics */
    USTAT       rem_r;                  /* Remote receive statistics */
} STAT;


/*
 * Results per node.
 */
typedef struct RESN {
    double      time_real;              /* Real (elapsed) time in seconds */
    double      time_cpu;               /* Cpu time in seconds */
    double      cpu_total;              /* Cpu time (as a fraction of a cpu) */
    double      cpu_user;               /* User time (fraction of cpu) */
    double      cpu_intr;               /* Interrupt time (fraction of cpu) */
    double      cpu_idle;               /* Idle time (fraction of cpu) */
    double      cpu_kernel;             /* Kernel time (fraction of cpu) */
    double      cpu_io_wait;            /* IO wait time (fraction of cpu) */
} RESN;


/*
 * Results.
 */
typedef struct RES {
    RESN        l;                      /* Local information */
    RESN        r;                      /* Remote information */
    double      send_bw;                /* Send bandwidth */
    double      recv_bw;                /* Receive bandwidth */
    double      msg_rate;               /* Messaging rate */
    double      send_cost;              /* Send cost */
    double      recv_cost;              /* Receive cost */
    double      latency;                /* Latency */
} RES;


/*
 * Functions prototypes.
 */
void        client_send_request(void);
void        debug(char *fmt, ...);
void        dec_init(void *p);
int64_t     dec_int(int n);
void        dec_str(char *s, int  n);
void        die(void);
void        enc_init(void *p);
void        enc_int(int64_t l, int n);
void        enc_str(char *s, int n);
int         error(char *fmt, ...);
void        error_die(char *fmt, ...);
void        exchange_results(void);
int         left_to_send(long *sentp, int room);
void        opt_check(void);
void       *qmalloc(long n);
int         recv_mesg(void *ptr, int len, char *item);
int         send_mesg(void *ptr, int len, char *item);
void        set_finished(void);
void        setp_u32(char *name, PAR_INDEX index, uint32_t l);
void        setp_str(char *name, PAR_INDEX index, char *s);
void        setv_u32(PAR_INDEX index, uint32_t l);
void        show_results(MEASURE measure);
void        stop_timing(void);
int         synchronize(void);
int         syserror(char *fmt, ...);
void        syserror_die(char *fmt, ...);
void        touch_data(void *p, int n);
void        par_use(PAR_INDEX index);


/*
 * Socket tests (ip.c).
 */
void    run_client_rds_bw(void);
void    run_server_rds_bw(void);
void    run_client_rds_lat(void);
void    run_server_rds_lat(void);
void    run_client_sdp_bw(void);
void    run_server_sdp_bw(void);
void    run_client_sdp_lat(void);
void    run_server_sdp_lat(void);
void    run_client_tcp_bw(void);
void    run_server_tcp_bw(void);
void    run_client_tcp_lat(void);
void    run_server_tcp_lat(void);
void    run_client_udp_bw(void);
void    run_server_udp_bw(void);
void    run_client_udp_lat(void);
void    run_server_udp_lat(void);


/*
 * InfiniBand tests (ib.c).
 */
void    run_client_bug(void);
void    run_server_bug(void);
void    run_client_rc_bi_bw(void);
void    run_server_rc_bi_bw(void);
void    run_client_rc_bw(void);
void    run_server_rc_bw(void);
void    run_client_rc_compare_swap_mr(void);
void    run_server_rc_compare_swap_mr(void);
void    run_client_rc_fetch_add_mr(void);
void    run_server_rc_fetch_add_mr(void);
void    run_client_rc_lat(void);
void    run_server_rc_lat(void);
void    run_client_rc_rdma_read_bw(void);
void    run_server_rc_rdma_read_bw(void);
void    run_client_rc_rdma_read_lat(void);
void    run_server_rc_rdma_read_lat(void);
void    run_client_rc_rdma_write_bw(void);
void    run_server_rc_rdma_write_bw(void);
void    run_client_rc_rdma_write_lat(void);
void    run_server_rc_rdma_write_lat(void);
void    run_client_rc_rdma_write_poll_lat(void);
void    run_server_rc_rdma_write_poll_lat(void);
void    run_client_uc_bi_bw(void);
void    run_server_uc_bi_bw(void);
void    run_client_uc_bw(void);
void    run_server_uc_bw(void);
void    run_client_uc_lat(void);
void    run_server_uc_lat(void);
void    run_client_uc_rdma_write_bw(void);
void    run_server_uc_rdma_write_bw(void);
void    run_client_uc_rdma_write_lat(void);
void    run_server_uc_rdma_write_lat(void);
void    run_client_uc_rdma_write_poll_lat(void);
void    run_server_uc_rdma_write_poll_lat(void);
void    run_client_ud_bi_bw(void);
void    run_server_ud_bi_bw(void);
void    run_client_ud_bw(void);
void    run_server_ud_bw(void);
void    run_client_ud_lat(void);
void    run_server_ud_lat(void);
void    run_client_ver_rc_compare_swap(void);
void    run_server_ver_rc_compare_swap(void);
void    run_client_ver_rc_fetch_add(void);
void    run_server_ver_rc_fetch_add(void);


/*
 * Variables.
 */
extern RES          Res;
extern REQ          Req;
extern STAT         LStat;
extern char        *Usage[];
extern char        *TestName;
extern char        *ServerName;
extern int          Successful;
extern volatile int Finished;
