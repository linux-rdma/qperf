/*
 * qperf - handle RDMA tests.
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <infiniband/verbs.h>
#include "qperf.h"


/*
 * RDMA parameters.
 */
#define QKEY            0x11111111      /* Q_Key */
#define NCQE            1024            /* Number of CQ entries */
#define GRH_SIZE        40              /* InfiniBand GRH size */
#define MTU_SIZE        2048            /* Default MTU Size */
#define RETRY_CNT       7               /* RC/UC retry count */
#define RNR_RETRY       7               /* RC/UC RNR retry count */
#define RNR_TIMER       12              /* RC/UC RNR timeout */
#define TIMEOUT         14              /* RC/UC timeout */


/*
 * Work request IDs.
 */
#define WRID_SEND       1               /* Send */
#define WRID_RECV       2               /* Receive */
#define WRID_RDMA       3               /* RDMA */


/*
 * Constants.
 */
#define K2      (2*1024)
#define K64     (64*1024)


/*
 * For convenience.
 */
typedef enum ibv_wr_opcode OPCODE;


/*
 * Atomics.
 */
typedef enum ATOMIC {
    FETCH_ADD,                          /* Fetch and add */
    COMPARE_SWAP                        /* Compare and swap */
} ATOMIC;


/*
 * IO Mode.
 */
typedef enum IOMODE {
    IO_SR,                              /* Send/Receive */
    IO_RDMA                             /* RDMA */
} IOMODE;


/*
 * RDMA connection context.
 */
typedef struct RCON {
    uint32_t    lid;                    /* Local ID */
    uint32_t    qpn;                    /* Queue pair number */
    uint32_t    psn;                    /* Packet sequence number */
    uint32_t    rkey;                   /* Remote key */
    uint64_t    vaddr;                  /* Virtual address */
} RCON;


/*
 * RDMA device descriptor.
 */
typedef struct RDEV {
    RCON                     lcon;      /* RDMA local context */
    RCON                     rcon;      /* RDMA remote context */
    int                      mtu;       /* MTU */
    int                      port;      /* Port */
    int                      rate;      /* Rate */
    int                      trans;     /* QP transport */
    int                      maxinline; /* Maximum amount of inline data */
    char                    *buffer;    /* Buffer */
    struct ibv_device       **devlist;  /* Device list */
    struct ibv_context      *context;   /* Context */
    struct ibv_comp_channel *channel;   /* Channel */
    struct ibv_pd           *pd;        /* Protection domain */
    struct ibv_mr           *mr;        /* Memory region */
    struct ibv_cq           *cq;        /* Completion queue */
    struct ibv_qp           *qp;        /* QPair */
    struct ibv_ah           *ah;        /* Address handle */
} RDEV;


/*
 * Names associated with a value.
 */
typedef struct NAMES {
    int     value;                       /* Value */
    char    *name;                       /* Name */
} NAMES;


/*
 * RDMA speeds and names.
 */
typedef struct RATES {
    const char *name;                   /* Name */
    uint32_t    rate;                   /* Rate */
} RATES;


/*
 * Function prototypes.
 */
static void     cq_error(int status);
static void     dec_ibcon(RCON *host);
static void     do_error(int status, uint64_t *errors);
static void     enc_ibcon(RCON *host);
static void     ib_bi_bw(int transport);
static void     ib_client_atomic(ATOMIC atomic);
static void     ib_client_bw(int transport);
static void     ib_client_rdma_bw(int transport, OPCODE opcode);
static void     ib_client_rdma_read_lat(int transport);
static void     ib_close(RDEV *rdev);
static void     ib_debug_info(RDEV *rdev);
static void     ib_init(RDEV *rdev);
static void     ib_mralloc(RDEV *rdev, int size);
static void     ib_open(RDEV *rdev, int trans, int maxSendWR, int maxRecvWR);
static void     ib_params_atomics(void);
static void     ib_params_msgs(long msgSize, int use_poll_mode);
static int      ib_poll(RDEV *rdev, struct ibv_wc *wc, int nwc);
static void     ib_post_rdma(RDEV *rdev, OPCODE opcode, int n);
static void     ib_post_compare_swap(RDEV *rdev,
                     int wrid, int offset, uint64_t compare, uint64_t swap);
static void     ib_post_fetch_add(RDEV *rdev,
                                        int wrid, int offset, uint64_t add);
static void     ib_post_recv(RDEV *rdev, int n);
static void     ib_post_send(RDEV *rdev, int n);
static void     ib_pp_lat(int transport, IOMODE iomode);
static void     ib_pp_lat_loop(RDEV *rdev, IOMODE iomode);
static void     ib_prepare(RDEV *rdev);
static void     ib_rdma_write_poll_lat(int transport);
static void     ib_server_def(int transport);
static void     ib_server_nop(int transport);
static int      maybe(int val, char *msg);
static char     *opcode_name(int opcode);


/*
 * List of errors we can get from a CQE.
 */
NAMES CQErrors[] ={
    { IBV_WC_SUCCESS,                   "Success"                       },
    { IBV_WC_LOC_LEN_ERR,               "Local length error"            },
    { IBV_WC_LOC_QP_OP_ERR,             "Local QP operation failure"    },
    { IBV_WC_LOC_EEC_OP_ERR,            "Local EEC operation failure"   },
    { IBV_WC_LOC_PROT_ERR,              "Local protection error"        },
    { IBV_WC_WR_FLUSH_ERR,              "WR flush failure"              },
    { IBV_WC_MW_BIND_ERR,               "Memory window bind failure"    },
    { IBV_WC_BAD_RESP_ERR,              "Bad response"                  },
    { IBV_WC_LOC_ACCESS_ERR,            "Local access failure"          },
    { IBV_WC_REM_INV_REQ_ERR,           "Remote invalid request"        },
    { IBV_WC_REM_ACCESS_ERR,            "Remote access failure"         },
    { IBV_WC_REM_OP_ERR,                "Remote operation failure"      },
    { IBV_WC_RETRY_EXC_ERR,             "Retries exceeded"              },
    { IBV_WC_RNR_RETRY_EXC_ERR,         "RNR retry exceeded"            },
    { IBV_WC_LOC_RDD_VIOL_ERR,          "Local RDD violation"           },
    { IBV_WC_REM_INV_RD_REQ_ERR,        "Remote invalid read request"   },
    { IBV_WC_REM_ABORT_ERR,             "Remote abort"                  },
    { IBV_WC_INV_EECN_ERR,              "Invalid EECN"                  },
    { IBV_WC_INV_EEC_STATE_ERR,         "Invalid EEC state"             },
    { IBV_WC_FATAL_ERR,                 "Fatal error"                   },
    { IBV_WC_RESP_TIMEOUT_ERR,          "Responder timeout"             },
    { IBV_WC_GENERAL_ERR,               "General error"                 },
};


/*
 * Opcodes.
 */
NAMES Opcodes[] ={
    { IBV_WR_ATOMIC_CMP_AND_SWP,        "compare and swap"              },
    { IBV_WR_ATOMIC_FETCH_AND_ADD,      "fetch and add"                 },
    { IBV_WR_RDMA_READ,                 "rdma read"                     },
    { IBV_WR_RDMA_WRITE,                "rdma write"                    },
    { IBV_WR_RDMA_WRITE_WITH_IMM,       "rdma write with immediate"     },
    { IBV_WR_SEND,                      "send"                          },
    { IBV_WR_SEND_WITH_IMM,             "send with immediate"           },
};


/*
 * Opcodes.
 */
RATES Rates[] ={
    { "",       IBV_RATE_MAX        },
    { "max",    IBV_RATE_MAX        },
    { "1xSDR",  IBV_RATE_2_5_GBPS   },
    { "1xDDR",  IBV_RATE_5_GBPS     },
    { "1xQDR",  IBV_RATE_10_GBPS    },
    { "4xSDR",  IBV_RATE_10_GBPS    },
    { "4xDDR",  IBV_RATE_20_GBPS    },
    { "4xQDR",  IBV_RATE_40_GBPS    },
    { "8xSDR",  IBV_RATE_20_GBPS    },
    { "8xDDR",  IBV_RATE_40_GBPS    },
    { "8xQDR",  IBV_RATE_80_GBPS    },
    { "2.5",    IBV_RATE_2_5_GBPS   },
    { "5",      IBV_RATE_5_GBPS     },
    { "10",     IBV_RATE_10_GBPS    },
    { "20",     IBV_RATE_20_GBPS    },
    { "30",     IBV_RATE_30_GBPS    },
    { "40",     IBV_RATE_40_GBPS    },
    { "60",     IBV_RATE_60_GBPS    },
    { "80",     IBV_RATE_80_GBPS    },
    { "120",    IBV_RATE_120_GBPS   },
};


/*
 * Measure RC bi-directional bandwidth (client side).
 */
void
run_client_rc_bi_bw(void)
{
    par_use(L_ACCESS_RECV);
    par_use(R_ACCESS_RECV);
    ib_params_msgs(K64, 1);
    ib_bi_bw(IBV_QPT_RC);
    show_results(BANDWIDTH);
}


/*
 * Measure RC bi-directional bandwidth (server side).
 */
void
run_server_rc_bi_bw(void)
{
    ib_bi_bw(IBV_QPT_RC);
}


/*
 * Measure RC bandwidth (client side).
 */
void
run_client_rc_bw(void)
{
    par_use(L_ACCESS_RECV);
    par_use(R_ACCESS_RECV);
    par_use(L_NO_MSGS);
    par_use(R_NO_MSGS);
    ib_params_msgs(K64, 1);
    ib_client_bw(IBV_QPT_RC);
    show_results(BANDWIDTH);
}


/*
 * Measure RC bandwidth (server side).
 */
void
run_server_rc_bw(void)
{
    ib_server_def(IBV_QPT_RC);
}


/*
 * Measure RC compare and swap messaging rate (client side).
 */
void
run_client_rc_compare_swap_mr(void)
{
    ib_client_atomic(COMPARE_SWAP);
}


/*
 * Measure RC compare and swap messaging rate (server side).
 */
void
run_server_rc_compare_swap_mr(void)
{
    ib_server_nop(IBV_QPT_RC);
}


/*
 * Measure RC fetch and add messaging rate (client side).
 */
void
run_client_rc_fetch_add_mr(void)
{
    ib_client_atomic(FETCH_ADD);
}


/*
 * Measure RC fetch and add messaging rate (server side).
 */
void
run_server_rc_fetch_add_mr(void)
{
    ib_server_nop(IBV_QPT_RC);
}


/*
 * Measure RC latency (client side).
 */
void
run_client_rc_lat(void)
{
    ib_params_msgs(1, 1);
    ib_pp_lat(IBV_QPT_RC, IO_SR);
}


/*
 * Measure RC latency (server side).
 */
void
run_server_rc_lat(void)
{
    ib_pp_lat(IBV_QPT_RC, IO_SR);
}


/*
 * Measure RC RDMA read bandwidth (client side).
 */
void
run_client_rc_rdma_read_bw(void)
{
    par_use(L_ACCESS_RECV);
    par_use(R_ACCESS_RECV);
    par_use(L_RD_ATOMIC);
    par_use(R_RD_ATOMIC);
    ib_params_msgs(K64, 1);
    ib_client_rdma_bw(IBV_QPT_RC, IBV_WR_RDMA_READ);
    show_results(BANDWIDTH);
}


/*
 * Measure RC RDMA read bandwidth (server side).
 */
void
run_server_rc_rdma_read_bw(void)
{
    ib_server_nop(IBV_QPT_RC);
}


/*
 * Measure RC RDMA read latency (client side).
 */
void
run_client_rc_rdma_read_lat(void)
{
    ib_params_msgs(1, 1);
    ib_client_rdma_read_lat(IBV_QPT_RC);
}


/*
 * Measure RC RDMA read latency (server side).
 */
void
run_server_rc_rdma_read_lat(void)
{
    ib_server_nop(IBV_QPT_RC);
}


/*
 * Measure RC RDMA write bandwidth (client side).
 */
void
run_client_rc_rdma_write_bw(void)
{
    ib_params_msgs(K64, 1);
    ib_client_rdma_bw(IBV_QPT_RC, IBV_WR_RDMA_WRITE_WITH_IMM);
    show_results(BANDWIDTH);
}


/*
 * Measure RC RDMA write bandwidth (server side).
 */
void
run_server_rc_rdma_write_bw(void)
{
    ib_server_def(IBV_QPT_RC);
}


/*
 * Measure RC RDMA write latency (client side).
 */
void
run_client_rc_rdma_write_lat(void)
{
    ib_params_msgs(1, 1);
    ib_pp_lat(IBV_QPT_RC, IO_RDMA);
}


/*
 * Measure RC RDMA write latency (server side).
 */
void
run_server_rc_rdma_write_lat(void)
{
    ib_pp_lat(IBV_QPT_RC, IO_RDMA);
}


/*
 * Measure RC RDMA write polling latency (client side).
 */
void
run_client_rc_rdma_write_poll_lat(void)
{
    ib_params_msgs(1, 0);
    ib_rdma_write_poll_lat(IBV_QPT_RC);
    show_results(LATENCY);
}


/*
 * Measure RC RDMA write polling latency (server side).
 */
void
run_server_rc_rdma_write_poll_lat(void)
{
    ib_rdma_write_poll_lat(IBV_QPT_RC);
}


/*
 * Measure UC bi-directional bandwidth (client side).
 */
void
run_client_uc_bi_bw(void)
{
    par_use(L_ACCESS_RECV);
    par_use(R_ACCESS_RECV);
    ib_params_msgs(K64, 1);
    ib_bi_bw(IBV_QPT_UC);
    show_results(BANDWIDTH_SR);
}


/*
 * Measure UC bi-directional bandwidth (server side).
 */
void
run_server_uc_bi_bw(void)
{
    ib_bi_bw(IBV_QPT_UC);
}


/*
 * Measure UC bandwidth (client side).
 */
void
run_client_uc_bw(void)
{
    par_use(L_ACCESS_RECV);
    par_use(R_ACCESS_RECV);
    par_use(L_NO_MSGS);
    par_use(R_NO_MSGS);
    ib_params_msgs(K64, 1);
    ib_client_bw(IBV_QPT_UC);
    show_results(BANDWIDTH_SR);
}


/*
 * Measure UC bandwidth (server side).
 */
void
run_server_uc_bw(void)
{
    ib_server_def(IBV_QPT_UC);
}


/*
 * Measure UC latency (client side).
 */
void
run_client_uc_lat(void)
{
    ib_params_msgs(1, 1);
    ib_pp_lat(IBV_QPT_UC, IO_SR);
}


/*
 * Measure UC latency (server side).
 */
void
run_server_uc_lat(void)
{
    ib_pp_lat(IBV_QPT_UC, IO_SR);
}


/*
 * Measure UC RDMA write bandwidth (client side).
 */
void
run_client_uc_rdma_write_bw(void)
{
    ib_params_msgs(K64, 1);
    ib_client_rdma_bw(IBV_QPT_UC, IBV_WR_RDMA_WRITE_WITH_IMM);
    show_results(BANDWIDTH_SR);
}


/*
 * Measure UC RDMA write bandwidth (server side).
 */
void
run_server_uc_rdma_write_bw(void)
{
    ib_server_def(IBV_QPT_UC);
}


/*
 * Measure UC RDMA write latency (client side).
 */
void
run_client_uc_rdma_write_lat(void)
{
    ib_params_msgs(1, 1);
    ib_pp_lat(IBV_QPT_UC, IO_RDMA);
}


/*
 * Measure UC RDMA write latency (server side).
 */
void
run_server_uc_rdma_write_lat(void)
{
    ib_pp_lat(IBV_QPT_UC, IO_RDMA);
}


/*
 * Measure UC RDMA write polling latency (client side).
 */
void
run_client_uc_rdma_write_poll_lat(void)
{
    ib_params_msgs(1, 1);
    ib_rdma_write_poll_lat(IBV_QPT_UC);
    show_results(LATENCY);
}


/*
 * Measure UC RDMA write polling latency (server side).
 */
void
run_server_uc_rdma_write_poll_lat(void)
{
    ib_rdma_write_poll_lat(IBV_QPT_UC);
}


/*
 * Measure UD bi-directional bandwidth (client side).
 */
void
run_client_ud_bi_bw(void)
{
    par_use(L_ACCESS_RECV);
    par_use(R_ACCESS_RECV);
    ib_params_msgs(K2, 1);
    ib_bi_bw(IBV_QPT_UD);
    show_results(BANDWIDTH_SR);
}


/*
 * Measure UD bi-directional bandwidth (server side).
 */
void
run_server_ud_bi_bw(void)
{
    ib_bi_bw(IBV_QPT_UD);
}


/*
 * Measure UD bandwidth (client side).
 */
void
run_client_ud_bw(void)
{
    par_use(L_ACCESS_RECV);
    par_use(R_ACCESS_RECV);
    par_use(L_NO_MSGS);
    par_use(R_NO_MSGS);
    ib_params_msgs(K2, 1);
    ib_client_bw(IBV_QPT_UD);
    show_results(BANDWIDTH_SR);
}


/*
 * Measure UD bandwidth (server side).
 */
void
run_server_ud_bw(void)
{
    ib_server_def(IBV_QPT_UD);
}


/*
 * Measure UD latency (client side).
 */
void
run_client_ud_lat(void)
{
    ib_params_msgs(1, 1);
    ib_pp_lat(IBV_QPT_UD, IO_SR);
}


/*
 * Measure UD latency (server side).
 */
void
run_server_ud_lat(void)
{
    ib_pp_lat(IBV_QPT_UD, IO_SR);
}

/*
 * Verify RC compare and swap (client side).
 */
void
run_client_ver_rc_compare_swap(void)
{
    int i;
    int size;
    RDEV rdev;
    uint64_t *result;
    uint64_t last = 0;
    uint64_t cur = 0;
    uint64_t next = 0x0123456789abcdefULL;

    ib_params_atomics();
    ib_open(&rdev, IBV_QPT_RC, NCQE, 0);
    size = Req.rd_atomic * sizeof(uint64_t);
    setv_u32(L_MSG_SIZE, size);
    setv_u32(R_MSG_SIZE, size);
    ib_mralloc(&rdev, size);
    ib_init(&rdev);
    sync_test();
    for (i = 0; i < Req.rd_atomic; ++i) {
        ib_post_compare_swap(&rdev, i, i*sizeof(uint64_t), cur, next);
        cur = next;
        next = cur + 1;
    }
    result = (uint64_t *) rdev.buffer;
    while (!Finished) {
        struct ibv_wc wc[NCQE];
        int n = ib_poll(&rdev, wc, cardof(wc));
        uint64_t res;

        if (Finished)
            break;
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        for (i = 0; i < n; ++i) {
            int x = wc[i].wr_id;
            int status = wc[i].status;
            if (status == IBV_WC_SUCCESS) {
                LStat.rem_r.no_bytes += sizeof(uint64_t);
                LStat.rem_r.no_msgs++;
            } else
                do_error(status, &LStat.s.no_errs);
            res = result[x];
            if (last != res)
                error(0, "compare and swap mismatch (expected %llx vs. %llx)",
                                            (long long)last, (long long)res);
            if (last)
                    last++;
            else
                    last = 0x0123456789abcdefULL;
            next = cur + 1;
            ib_post_compare_swap(&rdev, x, x*sizeof(uint64_t), cur, next);
            cur = next;
        }
    }
    stop_test_timer();
    exchange_results();
    ib_close(&rdev);
    show_results(MSG_RATE);
}


/*
 * Verify RC compare and swap (server side).
 */
void
run_server_ver_rc_compare_swap(void)
{
    ib_server_nop(IBV_QPT_RC);
}


/*
 * Verify RC fetch and add (client side).
 */
void
run_client_ver_rc_fetch_add(void)
{
    int i;
    int size;
    RDEV rdev;
    uint64_t *result;
    uint64_t last = 0;

    ib_params_atomics();
    ib_open(&rdev, IBV_QPT_RC, NCQE, 0);
    size = Req.rd_atomic * sizeof(uint64_t);
    setv_u32(L_MSG_SIZE, size);
    setv_u32(R_MSG_SIZE, size);
    ib_mralloc(&rdev, size);
    ib_init(&rdev);
    sync_test();
    for (i = 0; i < Req.rd_atomic; ++i)
        ib_post_fetch_add(&rdev, i, i*sizeof(uint64_t), 1);
    result = (uint64_t *) rdev.buffer;
    while (!Finished) {
        struct ibv_wc wc[NCQE];
        int n = ib_poll(&rdev, wc, cardof(wc));
        uint64_t res;

        if (Finished)
            break;
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        for (i = 0; i < n; ++i) {
            int x = wc[i].wr_id;
            int status = wc[i].status;
            if (status == IBV_WC_SUCCESS) {
                LStat.rem_r.no_bytes += sizeof(uint64_t);
                LStat.rem_r.no_msgs++;
            } else
                do_error(status, &LStat.s.no_errs);
            res = result[x];
            if (last != res)
                error(0, "fetch and add mismatch (expected %llx vs. %llx)",
                        (long long)last, (long long)res);
            last++;
            ib_post_fetch_add(&rdev, x, x*sizeof(uint64_t), 1);
        }
    }
    stop_test_timer();
    exchange_results();
    ib_close(&rdev);
    show_results(MSG_RATE);
}


/*
 * Verify RC fetch and add (server side).
 */
void
run_server_ver_rc_fetch_add(void)
{
    ib_server_nop(IBV_QPT_RC);
}


/*
 * Measure messaging rate for an atomic operation.
 */
static void
ib_client_atomic(ATOMIC atomic)
{
    int i;
    RDEV rdev;

    ib_params_atomics();
    ib_open(&rdev, IBV_QPT_RC, NCQE, 0);
    setv_u32(L_MSG_SIZE, sizeof(uint64_t));
    setv_u32(R_MSG_SIZE, sizeof(uint64_t));
    ib_mralloc(&rdev, sizeof(uint64_t));
    ib_init(&rdev);
    sync_test();

    for (i = 0; i < Req.rd_atomic; ++i) {
        if (atomic == FETCH_ADD)
            ib_post_fetch_add(&rdev, 0, 0, 0);
        else
            ib_post_compare_swap(&rdev, 0, 0, 0, 0);
    }

    while (!Finished) {
        struct ibv_wc wc[NCQE];
        int n = ib_poll(&rdev, wc, cardof(wc));
        if (Finished)
            break;
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        for (i = 0; i < n; ++i) {
            int status = wc[i].status;
            if (status == IBV_WC_SUCCESS) {
                LStat.rem_r.no_bytes += sizeof(uint64_t);
                LStat.rem_r.no_msgs++;
            } else
                do_error(status, &LStat.s.no_errs);
            if (atomic == FETCH_ADD)
                ib_post_fetch_add(&rdev, 0, 0, 0);
            else
                ib_post_compare_swap(&rdev, 0, 0, 0, 0);
        }
    }

    stop_test_timer();
    exchange_results();
    ib_close(&rdev);
    show_results(MSG_RATE);
}


/*
 * Measure RDMA bandwidth (client side).
 */
static void
ib_client_bw(int transport)
{
    RDEV rdev;

    long sent = 0;
    ib_open(&rdev, transport, NCQE, 0);
    ib_init(&rdev);
    sync_test();
    ib_post_send(&rdev, left_to_send(&sent, NCQE));
    sent = NCQE;
    while (!Finished) {
        int i;
        struct ibv_wc wc[NCQE];

        int n = ib_poll(&rdev, wc, cardof(wc));
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        if (Finished)
            break;
        for (i = 0; i < n; ++i) {
            int id = wc[i].wr_id;
            int status = wc[i].status;
            if (id != WRID_SEND)
                debug("bad WR ID %d", id);
            else if (status != IBV_WC_SUCCESS)
                do_error(status, &LStat.s.no_errs);
        }
        if (Req.no_msgs) {
            if (LStat.s.no_msgs + LStat.s.no_errs >= Req.no_msgs)
                break;
            n = left_to_send(&sent, n);
        }
        ib_post_send(&rdev, n);
        sent += n;
    }
    stop_test_timer();
    exchange_results();
    ib_close(&rdev);
}


/*
 * Default action for the server is to post receive buffers and whenever it
 * gets a completion entry, compute statistics and post more buffers.
 */
static void
ib_server_def(int transport)
{
    RDEV rdev;

    ib_open(&rdev, transport, 0, NCQE);
    ib_init(&rdev);
    ib_post_recv(&rdev, NCQE);
    sync_test();
    while (!Finished) {
        int i;
        struct ibv_wc wc[NCQE];
        int n = ib_poll(&rdev, wc, cardof(wc));
        if (Finished)
            break;
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        for (i = 0; i < n; ++i) {
            int status = wc[i].status;
            if (status == IBV_WC_SUCCESS) {
                LStat.r.no_bytes += Req.msg_size;
                LStat.r.no_msgs++;
                if (Req.access_recv)
                    touch_data(rdev.buffer, Req.msg_size);
            } else
                do_error(status, &LStat.r.no_errs);
        }
        if (Req.no_msgs)
            if (LStat.r.no_msgs + LStat.r.no_errs >= Req.no_msgs)
                break;
        ib_post_recv(&rdev, n);
    }
    stop_test_timer();
    exchange_results();
    ib_close(&rdev);
}


/*
 * Measure bi-directional RDMA bandwidth.
 */
static void
ib_bi_bw(int transport)
{
    RDEV rdev;

    ib_open(&rdev, transport, NCQE, NCQE);
    ib_init(&rdev);
    ib_post_recv(&rdev, NCQE);
    sync_test();
    ib_post_send(&rdev, NCQE);
    while (!Finished) {
        int i;
        struct ibv_wc wc[NCQE];
        int numSent = 0;
        int numRecv = 0;
        int n = ib_poll(&rdev, wc, cardof(wc));
        if (Finished)
            break;
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        for (i = 0; i < n; ++i) {
            int id = wc[i].wr_id;
            int status = wc[i].status;
            switch (id) {
            case WRID_SEND:
                if (status != IBV_WC_SUCCESS)
                    do_error(status, &LStat.s.no_errs);
                ++numSent;
                break;
            case WRID_RECV:
                if (status == IBV_WC_SUCCESS) {
                    LStat.r.no_bytes += Req.msg_size;
                    LStat.r.no_msgs++;
                    if (Req.access_recv)
                        touch_data(rdev.buffer, Req.msg_size);
                } else
                    do_error(status, &LStat.r.no_errs);
                ++numRecv;
                break;
            default:
                debug("bad WR ID %d", id);
            }
        }
        if (numRecv)
            ib_post_recv(&rdev, numRecv);
        if (numSent)
            ib_post_send(&rdev, numSent);
    }
    stop_test_timer();
    exchange_results();
    ib_close(&rdev);
}


/*
 * Measure ping-pong latency (client and server side).
 */
static void
ib_pp_lat(int transport, IOMODE iomode)
{
    RDEV rdev;

    ib_open(&rdev, transport, 1, 1);
    ib_init(&rdev);
    ib_pp_lat_loop(&rdev, iomode);
    stop_test_timer();
    exchange_results();
    ib_close(&rdev);
    if (is_client())
        show_results(LATENCY);
}


/*
 * Loop sending packets back and forth to measure ping-pong latency.
 */
static void
ib_pp_lat_loop(RDEV *rdev, IOMODE iomode)
{
    int done = 1;
    ib_post_recv(rdev, 1);
    sync_test();
    if (is_client()) {
        if (iomode == IO_SR)
            ib_post_send(rdev, 1);
        else
            ib_post_rdma(rdev, IBV_WR_RDMA_WRITE_WITH_IMM, 1);
        done = 0;
    }

    while (!Finished) {
        int i;
        struct ibv_wc wc[2];
        int n = ib_poll(rdev, wc, cardof(wc));
        if (Finished)
            break;
        for (i = 0; i < n; ++i) {
            int id = wc[i].wr_id;
            int status = wc[i].status;
            switch (id) {
            case WRID_SEND:
            case WRID_RDMA:
                if (status != IBV_WC_SUCCESS)
                    do_error(status, &LStat.s.no_errs);
                done |= 1;
                continue;
            case WRID_RECV:
                if (status == IBV_WC_SUCCESS) {
                    LStat.r.no_bytes += Req.msg_size;
                    LStat.r.no_msgs++;
                    ib_post_recv(rdev, 1);
                } else
                    do_error(status, &LStat.r.no_errs);
                done |= 2;
                continue;
            default:
                debug("bad WR ID %d", id);
                continue;
            }
            break;
        }
        if (done == 3) {
            if (iomode == IO_SR)
                ib_post_send(rdev, 1);
            else
                ib_post_rdma(rdev, IBV_WR_RDMA_WRITE_WITH_IMM, 1);
            done = 0;
        }
    }
}


/*
 * Loop sending packets back and forth using RDMA Write and polling to measure
 * latency.  Note that if we increase the number of entries of wc to be NCQE,
 * on the PS HCA, the latency is much longer.
 */
static void
ib_rdma_write_poll_lat(int transport)
{
    RDEV rdev;
    volatile char *p;
    volatile char *q;
    int send  = is_client() ? 1 : 0;
    int locID = send;
    int remID = !locID;

    ib_open(&rdev, transport, NCQE, 0);
    ib_init(&rdev);
    sync_test();
    p = &rdev.buffer[0];
    q = &rdev.buffer[Req.msg_size-1];
    while (!Finished) {
        *p = locID;
        *q = locID;
        if (send) {
            int i;
            int n;
            struct ibv_wc wc[2];

            ib_post_rdma(&rdev, IBV_WR_RDMA_WRITE, 1);
            if (Finished)
                break;
            n = ibv_poll_cq(rdev.cq, cardof(wc), wc);
            if (n < 0)
                error(SYS, "CQ poll failed");
            for (i = 0; i < n; ++i) {
                int id = wc[i].wr_id;
                int status = wc[i].status;
                if (id != WRID_RDMA)
                    debug("bad WR ID %d", id);
                else if (status != IBV_WC_SUCCESS)
                    do_error(status, &LStat.s.no_errs);
            }
        }
        while (!Finished)
            if (*p == remID && *q == remID)
                break;
        LStat.r.no_bytes += Req.msg_size;
        LStat.r.no_msgs++;
        send = 1;
    }
    stop_test_timer();
    exchange_results();
    ib_close(&rdev);
}


/*
 * Measure RDMA Read latency (client side).
 */
static void
ib_client_rdma_read_lat(int transport)
{
    RDEV rdev;

    ib_open(&rdev, transport, 1, 0);
    ib_init(&rdev);
    sync_test();
    ib_post_rdma(&rdev, IBV_WR_RDMA_READ, 1);
    while (!Finished) {
        struct ibv_wc wc;
        int n = ib_poll(&rdev, &wc, 1);
        if (n == 0)
            continue;
        if (Finished)
            break;
        if (wc.wr_id != WRID_RDMA) {
            debug("bad WR ID %d", (int)wc.wr_id);
            continue;
        }
        if (wc.status == IBV_WC_SUCCESS) {
            LStat.r.no_bytes += Req.msg_size;
            LStat.r.no_msgs++;
            LStat.rem_s.no_bytes += Req.msg_size;
            LStat.rem_s.no_msgs++;
        } else
            do_error(wc.status, &LStat.s.no_errs);
        ib_post_rdma(&rdev, IBV_WR_RDMA_READ, 1);
    }
    stop_test_timer();
    exchange_results();
    ib_close(&rdev);
    show_results(LATENCY);
}


/*
 * Measure RDMA bandwidth (client side).
 */
static void
ib_client_rdma_bw(int transport, OPCODE opcode)
{
    RDEV rdev;

    ib_open(&rdev, transport, NCQE, 0);
    ib_init(&rdev);
    sync_test();
    ib_post_rdma(&rdev, opcode, NCQE);
    while (!Finished) {
        int i;
        struct ibv_wc wc[NCQE];
        int n = ib_poll(&rdev, wc, cardof(wc));
        if (Finished)
            break;
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        for (i = 0; i < n; ++i) {
            int status = wc[i].status;
            if (status == IBV_WC_SUCCESS) {
                if (opcode == IBV_WR_RDMA_READ) {
                    LStat.r.no_bytes += Req.msg_size;
                    LStat.r.no_msgs++;
                    LStat.rem_s.no_bytes += Req.msg_size;
                    LStat.rem_s.no_msgs++;
                    if (Req.access_recv)
                        touch_data(rdev.buffer, Req.msg_size);
                }
            } else
                do_error(status, &LStat.s.no_errs);
        }
        ib_post_rdma(&rdev, opcode, n);
    }
    stop_test_timer();
    exchange_results();
    ib_close(&rdev);
}


/*
 * Server just waits and lets driver take care of any requests.
 */
static void
ib_server_nop(int transport)
{
    RDEV rdev;

    /* workaround: Size of RQ should be 0; bug in Mellanox driver */
    ib_open(&rdev, transport, 0, 1);
    ib_init(&rdev);
    sync_test();
    while (!Finished)
        pause();
    stop_test_timer();
    exchange_results();
    ib_close(&rdev);
}


/*
 * Set default RDMA parameters for tests that use messages.
 */
static void
ib_params_msgs(long msgSize, int use_poll_mode)
{
    setp_u32(0, L_MSG_SIZE, msgSize);
    setp_u32(0, R_MSG_SIZE, msgSize);
    setp_u32(0, L_MTU_SIZE, MTU_SIZE);
    setp_u32(0, R_MTU_SIZE, MTU_SIZE);
    par_use(L_ID);
    par_use(R_ID);
    par_use(L_MTU_SIZE);
    par_use(R_MTU_SIZE);
    par_use(L_SL);
    par_use(R_SL);
    par_use(L_STATIC_RATE);
    par_use(R_STATIC_RATE);
    if (use_poll_mode) {
        par_use(L_POLL_MODE);
        par_use(R_POLL_MODE);
    }
    opt_check();
}


/*
 * Set default RDMA parameters for tests that use atomics.
 */
static void
ib_params_atomics(void)
{
    setp_u32(0, L_MTU_SIZE, MTU_SIZE);
    setp_u32(0, R_MTU_SIZE, MTU_SIZE);
    par_use(L_ID);
    par_use(R_ID);
    par_use(L_POLL_MODE);
    par_use(R_POLL_MODE);
    par_use(L_RD_ATOMIC);
    par_use(R_RD_ATOMIC);
    par_use(L_SL);
    par_use(R_SL);
    par_use(L_STATIC_RATE);
    par_use(R_STATIC_RATE);
    opt_check();

    setv_u32(L_MSG_SIZE, 0);
}


/*
 * RDMA initialization.
 */
static void
ib_init(RDEV *rdev)
{
    RCON ibcon;

    if (is_client()) {
        client_send_request();
        enc_init(&ibcon);
        enc_ibcon(&rdev->lcon);
        send_mesg(&ibcon, sizeof(ibcon), "RDMA connection");
        recv_mesg(&ibcon, sizeof(ibcon), "RDMA connection");
        dec_init(&ibcon);
        dec_ibcon(&rdev->rcon);
    } else {
        recv_mesg(&ibcon, sizeof(ibcon), "RDMA connection");
        dec_init(&ibcon);
        dec_ibcon(&rdev->rcon);
        enc_init(&ibcon);
        enc_ibcon(&rdev->lcon);
        send_mesg(&ibcon, sizeof(ibcon), "RDMA connection");
    }
    ib_prepare(rdev);
    ib_debug_info(rdev);
}


/*
 * Show debugging information.
 */
static void
ib_debug_info(RDEV *rdev)
{
    debug("L: lid=%04x qpn=%06x psn=%06x rkey=%08x vaddr=%010x",
                    rdev->lcon.lid, rdev->lcon.qpn, rdev->lcon.psn,
                                    rdev->lcon.rkey, rdev->lcon.vaddr);
    debug("R: lid=%04x qpn=%06x psn=%06x rkey=%08x vaddr=%010x",
                    rdev->rcon.lid, rdev->rcon.qpn, rdev->rcon.psn,
                                    rdev->rcon.rkey, rdev->rcon.vaddr);
}


/*
 * Open a RDMA device.
 */
static void
ib_open(RDEV *rdev, int trans, int maxSendWR, int maxRecvWR)
{
    /* Clear structure */
    memset(rdev, 0, sizeof(*rdev));

    /* Check and set MTU */
    {
        int mtu = Req.mtu_size;
        if (mtu == 256)
            rdev->mtu = IBV_MTU_256;
        else if (mtu == 512)
            rdev->mtu = IBV_MTU_512;
        else if (mtu == 1024)
            rdev->mtu = IBV_MTU_1024;
        else if (mtu == 2048)
            rdev->mtu = IBV_MTU_2048;
        else if (mtu == 4096)
            rdev->mtu = IBV_MTU_4096;
        else
            error(0, "bad MTU: %d; must be 256/512/1K/2K/4K", mtu);
    }

    /* Set transport type */
    rdev->trans = trans;

    /* Set port */
    {
        int port = 1;
        char *p = index(Req.id, ':');
        if (p) {
            *p++ = '\0';
            port = atoi(p);
            if (port < 1)
                error(0, "bad IB port: %d; must be at least 1", port);
        }
        rdev->port = port;
    }

    /* Set rate */
    {
        RATES *q = Rates;
        RATES *r = q + cardof(Rates);

        for (;; ++q) {
            if (q >= r)
                error(SYS, "bad static rate: %s", Req.static_rate);
            if (streq(Req.static_rate, q->name)) {
                rdev->rate = q->rate;
                break;
            }
        }
    }

    /* Determine device and open */
    {
        struct ibv_device *device;
        char *name = Req.id[0] ? Req.id : 0;

        rdev->devlist = ibv_get_device_list(0);
        if (!rdev->devlist)
            error(SYS, "failed to find any RDMA devices");
        if (!name)
            device = *rdev->devlist;
        else {
            struct ibv_device **d = rdev->devlist;
            while ((device = *d++))
                if (streq(ibv_get_device_name(device), name))
                    break;
        }
        if (!device)
            error(SYS, "failed to find RDMA device");
        rdev->context = ibv_open_device(device);
        if (!rdev->context)
            error(SYS, "failed to open device %s", ibv_get_device_name(device));
    }

    /* Allocate completion channel */
    rdev->channel = ibv_create_comp_channel(rdev->context);
    if (!rdev->channel)
        error(SYS, "failed to create completion channel");

    /* Allocate protection domain */
    rdev->pd = ibv_alloc_pd(rdev->context);
    if (!rdev->pd)
        error(SYS, "failed to allocate protection domain");

    /* Allocate message buffer and memory region */
    {
        int bufSize = Req.msg_size;
        int pageSize = sysconf(_SC_PAGESIZE);
        if (trans == IBV_QPT_UD)
            bufSize += GRH_SIZE;
        if (bufSize == 0)
            bufSize = 1;
        if (posix_memalign((void **)&rdev->buffer, pageSize, bufSize) != 0)
            error(SYS, "failed to allocate memory");
        memset(rdev->buffer, 0, bufSize);
        int flags = IBV_ACCESS_LOCAL_WRITE  |
                    IBV_ACCESS_REMOTE_READ  |
                    IBV_ACCESS_REMOTE_WRITE |
                    IBV_ACCESS_REMOTE_ATOMIC;
        rdev->mr = ibv_reg_mr(rdev->pd, rdev->buffer, bufSize, flags);
        if (!rdev->mr)
            error(SYS, "failed to allocate memory region");
    }

    /* Create completion queue */
    rdev->cq = ibv_create_cq(rdev->context,
                              maxSendWR+maxRecvWR, 0, rdev->channel, 0);
    if (!rdev->cq)
        error(SYS, "failed to create completion queue");

    /* Create queue pair */
    {
        struct ibv_qp_init_attr attr ={
            .send_cq = rdev->cq,
            .recv_cq = rdev->cq,
            .cap     ={
                .max_send_wr        = maxSendWR,
                .max_recv_wr        = maxRecvWR,
                .max_send_sge       = 1,
                .max_recv_sge       = 1,
                .max_inline_data    = 0,
            },
            .qp_type = rdev->trans,
        };
        rdev->qp = ibv_create_qp(rdev->pd, &attr);
        if (!rdev->qp)
            error(SYS, "failed to create QP");
    }

    /* Modify queue pair to INIT state */
    {
        struct ibv_qp_attr attr ={
            .qp_state       = IBV_QPS_INIT,
            .pkey_index     = 0,
            .port_num       = rdev->port
        };
        int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT;

        if (rdev->trans == IBV_QPT_UD) {
            flags |= IBV_QP_QKEY;
            attr.qkey = QKEY;
        } else if (rdev->trans == IBV_QPT_RC) {
            flags |= IBV_QP_ACCESS_FLAGS;
            attr.qp_access_flags =
                IBV_ACCESS_REMOTE_READ  |
                IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_ATOMIC;
        } else if (rdev->trans == IBV_QPT_UC) {
            flags |= IBV_QP_ACCESS_FLAGS;
            attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
        }
        if (ibv_modify_qp(rdev->qp, &attr, flags) != SUCCESS0)
            error(SYS, "failed to modify QP to INIT state");
    }

    /* Get QP attributes */
    {
        struct ibv_qp_attr qp_attr;
        struct ibv_qp_init_attr qp_init_attr;

        if (ibv_query_qp(rdev->qp, &qp_attr, 0, &qp_init_attr) != SUCCESS0)
            error(SYS, "query QP failed");
        rdev->maxinline = qp_attr.cap.max_inline_data;
    }

    /* Get device properties */
    {
        struct ibv_device_attr dev_attr;

        if (ibv_query_device(rdev->context, &dev_attr) != SUCCESS0)
            error(SYS, "query device failed");
        if (Req.rd_atomic == 0)
            Req.rd_atomic = dev_attr.max_qp_rd_atom;
        else if (Req.rd_atomic > dev_attr.max_qp_rd_atom)
            error(0, "device only supports %d (< %d) RDMA reads or atomic ops",
                                    dev_attr.max_qp_rd_atom, Req.rd_atomic);
    }

    /* Set up local context */
    {
        struct ibv_port_attr port_attr;

        int stat = ibv_query_port(rdev->context, rdev->port, &port_attr);
        if (stat != SUCCESS0)
            error(SYS, "query port failed");
        srand48(getpid()*time(0));

        rdev->lcon.lid   = port_attr.lid;
        rdev->lcon.qpn   = rdev->qp->qp_num;
        rdev->lcon.psn   = lrand48() & 0xffffff;
        rdev->lcon.rkey  = 0;
        rdev->lcon.vaddr = 0;
    }

    /* Allocate memory region */
    ib_mralloc(rdev, Req.msg_size);
}


/*
 * Allocate a memory region.
 */
static void
ib_mralloc(RDEV *rdev, int size)
{
    int pageSize;

    if (size == 0)
        return;
    if (rdev->trans == IBV_QPT_UD)
        size += GRH_SIZE;
    pageSize = sysconf(_SC_PAGESIZE);
    if (posix_memalign((void **)&rdev->buffer, pageSize, size) != 0)
        error(SYS, "failed to allocate memory");
    memset(rdev->buffer, 0, size);
    int flags = IBV_ACCESS_LOCAL_WRITE  |
                IBV_ACCESS_REMOTE_READ  |
                IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_ATOMIC;
    rdev->mr = ibv_reg_mr(rdev->pd, rdev->buffer, size, flags);
    if (!rdev->mr)
        error(SYS, "failed to allocate memory region");

    rdev->lcon.rkey  = rdev->mr->rkey;
    rdev->lcon.vaddr = (unsigned long)rdev->buffer;
}


/*
 * Prepare the RDMA device for receiving and sending.
 */
static void
ib_prepare(RDEV *rdev)
{
    int flags;
    struct ibv_qp_attr rtr_attr ={
        .qp_state           = IBV_QPS_RTR,
        .path_mtu           = rdev->mtu,
        .dest_qp_num        = rdev->rcon.qpn,
        .rq_psn             = rdev->rcon.psn,
        .min_rnr_timer      = RNR_TIMER,
        .max_dest_rd_atomic = Req.rd_atomic,
        .ah_attr            = {
            .dlid           = rdev->rcon.lid,
            .port_num       = rdev->port,
            .static_rate    = rdev->rate,
            .sl             = Req.sl
        }
    };
    struct ibv_qp_attr rts_attr ={
        .qp_state       = IBV_QPS_RTS,
        .timeout        = TIMEOUT,
        .retry_cnt      = RETRY_CNT,
        .rnr_retry      = RNR_RETRY,
        .sq_psn         = rdev->lcon.psn,
        .max_rd_atomic  = Req.rd_atomic
    };
    struct ibv_ah_attr ah_attr ={
        .dlid           = rdev->rcon.lid,
        .port_num       = rdev->port,
        .static_rate    = rdev->rate,
        .sl             = Req.sl
    };

    if (rdev->trans == IBV_QPT_UD) {
        /* Modify queue pair to RTR */
        flags = IBV_QP_STATE;
        if (ibv_modify_qp(rdev->qp, &rtr_attr, flags) != SUCCESS0)
            error(SYS, "failed to modify QP to RTR");

        /* Modify queue pair to RTS */
        flags = IBV_QP_STATE | IBV_QP_SQ_PSN;
        if (ibv_modify_qp(rdev->qp, &rts_attr, flags) != SUCCESS0)
            error(SYS, "failed to modify QP to RTS");

        /* Create address handle */
        rdev->ah = ibv_create_ah(rdev->pd, &ah_attr);
        if (!rdev->ah)
            error(SYS, "failed to create address handle");
    } else if (rdev->trans == IBV_QPT_RC) {
        /* Modify queue pair to RTR */
        flags = IBV_QP_STATE              |
                IBV_QP_AV                 |
                IBV_QP_PATH_MTU           |
                IBV_QP_DEST_QPN           |
                IBV_QP_RQ_PSN             |
                IBV_QP_MAX_DEST_RD_ATOMIC |
                IBV_QP_MIN_RNR_TIMER;
        if (ibv_modify_qp(rdev->qp, &rtr_attr, flags) != SUCCESS0)
            error(SYS, "failed to modify QP to RTR");

        /* Modify queue pair to RTS */
        flags = IBV_QP_STATE     |
                IBV_QP_TIMEOUT   |
                IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY |
                IBV_QP_SQ_PSN    |
                IBV_QP_MAX_QP_RD_ATOMIC;
        if (ibv_modify_qp(rdev->qp, &rts_attr, flags) != SUCCESS0)
            error(SYS, "failed to modify QP to RTS");
    } else if (rdev->trans == IBV_QPT_UC) {
        /* Modify queue pair to RTR */
        flags = IBV_QP_STATE    |
                IBV_QP_AV       |
                IBV_QP_PATH_MTU |
                IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN;
        if (ibv_modify_qp(rdev->qp, &rtr_attr, flags) != SUCCESS0)
            error(SYS, "failed to modify QP to RTR");

        /* Modify queue pair to RTS */
        flags = IBV_QP_STATE |
                IBV_QP_SQ_PSN;
        if (ibv_modify_qp(rdev->qp, &rts_attr, flags) != SUCCESS0)
            error(SYS, "failed to modify QP to RTS");
    }
    if (!Req.poll_mode) {
        if (ibv_req_notify_cq(rdev->cq, 0) != SUCCESS0)
            error(SYS, "failed to request CQ notification");
    }
}


/*
 * Close a RDMA device.  We ust destroy the CQ before the QP otherwise the
 * ibv_destroy_qp call might hang.
 */
static void
ib_close(RDEV *rdev)
{
    if (rdev->ah)
        ibv_destroy_ah(rdev->ah);
    if (rdev->cq)
        ibv_destroy_cq(rdev->cq);
    if (rdev->qp)
        ibv_destroy_qp(rdev->qp);
    if (rdev->mr)
        ibv_dereg_mr(rdev->mr);
    if (rdev->pd)
        ibv_dealloc_pd(rdev->pd);
    if (rdev->channel)
        ibv_destroy_comp_channel(rdev->channel);
    if (rdev->context)
        ibv_close_device(rdev->context);
    if (rdev->buffer)
        free(rdev->buffer);
    if (rdev->devlist)
        free(rdev->devlist);
    memset(rdev, 0, sizeof(*rdev));
}


/*
 * Post a compare and swap request.
 */
static void
ib_post_compare_swap(RDEV *rdev,
                     int wrid, int offset, uint64_t compare, uint64_t swap)
{
    struct ibv_sge sge ={
        .addr   = (uintptr_t)rdev->buffer + offset,
        .length = sizeof(uint64_t),
        .lkey   = rdev->mr->lkey
    };
    struct ibv_send_wr wr ={
        .wr_id      = wrid,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_ATOMIC_CMP_AND_SWP,
        .send_flags = IBV_SEND_SIGNALED,
        .wr = {
            .atomic = {
                .remote_addr    = rdev->rcon.vaddr,
                .rkey           = rdev->rcon.rkey,
                .compare_add    = compare,
                .swap           = swap
            }
        }
    };
    struct ibv_send_wr *badWR;

    errno = 0;
    if (ibv_post_send(rdev->qp, &wr, &badWR) != SUCCESS0) {
        if (Finished && errno == EINTR)
            return;
        error(SYS, "failed to post compare and swap");
    }

    LStat.s.no_bytes += sizeof(uint64_t);
    LStat.s.no_msgs++;
}


/*
 * Post a fetch and add request.
 */
static void
ib_post_fetch_add(RDEV *rdev, int wrid, int offset, uint64_t add)
{
    struct ibv_sge sge ={
        .addr   = (uintptr_t) rdev->buffer + offset,
        .length = sizeof(uint64_t),
        .lkey   = rdev->mr->lkey
    };
    struct ibv_send_wr wr ={
        .wr_id      = wrid,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_ATOMIC_FETCH_AND_ADD,
        .send_flags = IBV_SEND_SIGNALED,
        .wr = {
            .atomic = {
                .remote_addr    = rdev->rcon.vaddr,
                .rkey           = rdev->rcon.rkey,
                .compare_add    = add
            }
        }
    };
    struct ibv_send_wr *badWR;

    errno = 0;
    if (ibv_post_send(rdev->qp, &wr, &badWR) != SUCCESS0) {
        if (Finished && errno == EINTR)
            return;
        error(SYS, "failed to post fetch and add");
    }

    LStat.s.no_bytes += sizeof(uint64_t);
    LStat.s.no_msgs++;
}


/*
 * Post n sends.
 */
static void
ib_post_send(RDEV *rdev, int n)
{
    struct ibv_sge sge ={
        .addr   = (uintptr_t) rdev->buffer,
        .length = Req.msg_size,
        .lkey   = rdev->mr->lkey
    };
    struct ibv_send_wr wr ={
        .wr_id      = WRID_SEND,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
    };
    struct ibv_send_wr *badWR;

    if (rdev->trans == IBV_QPT_UD) {
        wr.wr.ud.ah          = rdev->ah;
        wr.wr.ud.remote_qpn  = rdev->rcon.qpn;
        wr.wr.ud.remote_qkey = QKEY;
    }
    if (Req.msg_size <= rdev->maxinline)
        wr.send_flags |= IBV_SEND_INLINE;
    errno = 0;
    while (n-- > 0) {
        if (ibv_post_send(rdev->qp, &wr, &badWR) != SUCCESS0) {
            if (Finished && errno == EINTR)
                return;
            error(SYS, "failed to post send");
        }
        LStat.s.no_bytes += Req.msg_size;
        LStat.s.no_msgs++;
    }
}


/*
 * Post n receives.
 */
static void
ib_post_recv(RDEV *rdev, int n)
{
    struct ibv_sge sge ={
        .addr   = (uintptr_t) rdev->buffer,
        .length = Req.msg_size,
        .lkey   = rdev->mr->lkey
    };
    struct ibv_recv_wr wr ={
        .wr_id      = WRID_RECV,
        .sg_list    = &sge,
        .num_sge    = 1,
    };
    struct ibv_recv_wr *badWR;

    if (rdev->trans == IBV_QPT_UD)
        sge.length += GRH_SIZE;

    errno = 0;
    while (n-- > 0) {
        if (ibv_post_recv(rdev->qp, &wr, &badWR) != SUCCESS0) {
            if (Finished && errno == EINTR)
                return;
            error(SYS, "failed to post receive");
        }
    }
}


/*
 * Post n RDMA requests.
 */
static void
ib_post_rdma(RDEV *rdev, OPCODE opcode, int n)
{
    struct ibv_sge sge ={
        .addr   = (uintptr_t) rdev->buffer,
        .length = Req.msg_size,
        .lkey   = rdev->mr->lkey
    };
    struct ibv_send_wr wr ={
        .wr_id      = WRID_RDMA,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = opcode,
        .send_flags = IBV_SEND_SIGNALED,
        .wr = {
            .rdma = {
                .remote_addr = rdev->rcon.vaddr,
                .rkey        = rdev->rcon.rkey
            }
        }
    };
    struct ibv_send_wr *badWR;

    if (opcode != IBV_WR_RDMA_READ && Req.msg_size <= rdev->maxinline)
        wr.send_flags |= IBV_SEND_INLINE;
    errno = 0;
    while (n--) {
        if (ibv_post_send(rdev->qp, &wr, &badWR) != SUCCESS0) {
            if (Finished && errno == EINTR)
                return;
            error(SYS, "failed to post %s", opcode_name(wr.opcode));
        }
        if (opcode != IBV_WR_RDMA_READ) {
            LStat.s.no_bytes += Req.msg_size;
            LStat.s.no_msgs++;
        }
    }
}


/*
 * Poll the completion queue.
 */
static int
ib_poll(RDEV *rdev, struct ibv_wc *wc, int nwc)
{
    int n;

    if (!Req.poll_mode && !Finished) {
        void *ectx;
        struct ibv_cq *ecq;

        if (ibv_get_cq_event(rdev->channel, &ecq, &ectx) != SUCCESS0)
            return maybe(0, "failed to get CQ event");
        if (ecq != rdev->cq)
            error(0, "CQ event for unknown CQ");
        if (ibv_req_notify_cq(rdev->cq, 0) != SUCCESS0)
            return maybe(0, "failed to request CQ notification");
    }
    n = ibv_poll_cq(rdev->cq, nwc, wc);
    if (n < 0)
        return maybe(0, "CQ poll failed");
    return n;
}


/*
 * We encountered an error in a system call which might simply have been
 * interrupted by the alarm that signaled completion of the test.  Generate the
 * error if appropriate or return the requested value.  Final return is just to
 * silence the compiler.
 */
static int
maybe(int val, char *msg)
{
    if (Finished && errno == EINTR)
        return val;
    error(SYS, msg);
    return 0;
}


/*
 * Encode a RCON structure into a data stream.
 */
static void
enc_ibcon(RCON *host)
{
    enc_int(host->lid,   sizeof(host->lid));
    enc_int(host->qpn,   sizeof(host->qpn));
    enc_int(host->psn,   sizeof(host->psn));
    enc_int(host->rkey,  sizeof(host->rkey));
    enc_int(host->vaddr, sizeof(host->vaddr));
}


/*
 * Decode a RCON structure from a data stream.
 */
static void
dec_ibcon(RCON *host)
{
    host->lid   = dec_int(sizeof(host->lid));
    host->qpn   = dec_int(sizeof(host->qpn));
    host->psn   = dec_int(sizeof(host->psn));
    host->rkey  = dec_int(sizeof(host->rkey));
    host->vaddr = dec_int(sizeof(host->vaddr));
}


/*
 * Handle a CQ error and return true if it is recoverable.
 */
static void
do_error(int status, uint64_t *errors)
{
    ++*errors;
    cq_error(status);
}


/*
 * Print out a CQ error given a status.
 */
static void
cq_error(int status)
{
    int i;

    for (i = 0; i < cardof(CQErrors); ++i)
        if (CQErrors[i].value == status)
            error(0, "%s failed: %s", TestName, CQErrors[i].name);
    error(0, "%s failed: CQ error %d", TestName, status);
}


/*
 * Return the name of an opcode.
 */
static char *
opcode_name(int opcode)
{
    int i;

    for (i = 0; i < cardof(Opcodes); ++i)
        if (Opcodes[i].value == opcode)
            return Opcodes[i].name;
    return "unknown operation";
}
