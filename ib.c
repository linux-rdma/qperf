/*
 * qperf - handle RDMA tests.
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
#define GRH_SIZE        40              /* IB GRH size */
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
typedef struct IBCON {
    uint32_t    lid;                    /* Local ID */
    uint32_t    qpn;                    /* Queue pair number */
    uint32_t    psn;                    /* Packet sequence number */
    uint32_t    rkey;                   /* Remote key */
    uint64_t    vaddr;                  /* Virtual address */
} IBCON;


/*
 * RDMA descriptor.
 */
typedef struct IBDEV {
    IBCON                   lcon;       /* Local context */
    IBCON                   rcon;       /* Remote context */
    int                     mtu;        /* MTU */
    int                     port;       /* Port */
    int                     rate;       /* Rate */
    int                     trans;      /* QP transport */
    int                     maxinline;  /* Maximum amount of inline data */
    char                    *buffer;    /* Buffer */
    struct ibv_device       **devlist;  /* Device list */
    struct ibv_context      *context;   /* Context */
    struct ibv_comp_channel *channel;   /* Channel */
    struct ibv_pd           *pd;        /* Protection domain */
    struct ibv_mr           *mr;        /* Memory region */
    struct ibv_cq           *cq;        /* Completion queue */
    struct ibv_qp           *qp;        /* QPair */
    struct ibv_ah           *ah;        /* Address handle */
} IBDEV;


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
static void     dec_ibcon(IBCON *host);
static int      do_error(int status, uint64_t *errors);
static void     enc_ibcon(IBCON *host);
static void     ib_bi_bw(int transport);
static void     ib_client_atomic(ATOMIC atomic);
static void     ib_client_bw(int transport);
static void     ib_client_rdma_bw(int transport, OPCODE opcode);
static void     ib_client_rdma_read_lat(int transport);
static void     ib_close(IBDEV *ibdev);
static void     ib_debug_info(IBDEV *ibdev);
static int      ib_init(IBDEV *ibdev);
static int      ib_mralloc(IBDEV *ibdev, int size);
static int      ib_open(IBDEV *ibdev, int trans, int maxSendWR, int maxRecvWR);
static void     ib_params_atomics(void);
static void     ib_params_msgs(long msgSize, int use_poll_mode);
static int      ib_poll(IBDEV *ibdev, struct ibv_wc *wc, int nwc);
static int      ib_post_rdma(IBDEV *ibdev, OPCODE opcode, int n);
static int      ib_post_compare_swap(IBDEV *ibdev,
                     int wrid, int offset, uint64_t compare, uint64_t swap);
static int      ib_post_fetch_add(IBDEV *ibdev,
                                        int wrid, int offset, uint64_t add);
static int      ib_post_recv(IBDEV *ibdev, int n);
static int      ib_post_send(IBDEV *ibdev, int n);
static void     ib_pp_lat(int transport, IOMODE iomode);
static void     ib_pp_lat_loop(IBDEV *ibdev, IOMODE iomode);
static int      ib_prepare(IBDEV *ibdev);
static void     ib_rdma_write_poll_lat(int transport);
static void     ib_server_def(int transport);
static void     ib_server_nop(int transport);
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
 * Experimental (client side).
 */
void
run_client_experimental(void)
{
    IBDEV ibdev;

    ib_params_msgs(K64, 1);
    if (!ib_open(&ibdev, IBV_QPT_UC, 1, 0))
        goto err;
    if (!ib_init(&ibdev))
        goto err;
    if (!synchronize())
        goto err;
    if (!ib_post_rdma(&ibdev, IBV_WR_RDMA_WRITE_WITH_IMM, 1))
        goto err;
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
}


/*
 * Experimental (server side).
 */
void
run_server_experimental(void)
{
    IBDEV ibdev;
    int found = 0;

    if (!ib_open(&ibdev, IBV_QPT_UC, 0, 1))
        return;
    if (!ib_init(&ibdev))
        goto err;
    if (!ib_post_recv(&ibdev, 1))
        goto err;
    if (!synchronize())
        goto err;
    while (!Finished) {
        struct ibv_wc wc[NCQE];
        int n = ib_poll(&ibdev, wc, cardof(wc));
        if (n < 0)
            goto err;
        if (n) {
            found = 1;
            break;
        }
    }
    if (found)
        printf("Received immediate data\n");
    else
        printf("Failed to received immediate data\n");
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
}


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
    IBDEV ibdev;
    uint64_t *result;
    uint64_t last = 0;
    uint64_t cur = 0;
    uint64_t next = 0x0123456789abcdefULL;
    int i;
    int size;

    ib_params_atomics();
    if (!ib_open(&ibdev, IBV_QPT_RC, NCQE, 0))
        goto err;
    size = Req.rd_atomic * sizeof(uint64_t);
    setv_u32(L_MSG_SIZE, size);
    setv_u32(R_MSG_SIZE, size);
    ib_mralloc(&ibdev, size);
    if (!ib_init(&ibdev))
        goto err;
    if (!synchronize())
        goto err;
    for (i = 0; i < Req.rd_atomic; ++i) {
        if (!ib_post_compare_swap(&ibdev, i, i*sizeof(uint64_t), cur, next))
            goto err;
        cur = next;
        next = cur + 1;
    }
    result = (uint64_t *) ibdev.buffer;
    while (!Finished) {
        struct ibv_wc wc[NCQE];
        int n = ib_poll(&ibdev, wc, cardof(wc));
        uint64_t res;

        if (Finished)
            break;
        if (n < 0)
            goto err;
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        for (i = 0; i < n; ++i) {
            int x = wc[i].wr_id;
            int status = wc[i].status;
            if (status == IBV_WC_SUCCESS) {
                LStat.rem_r.no_bytes += sizeof(uint64_t);
                LStat.rem_r.no_msgs++;
            } else if (!do_error(status, &LStat.s.no_errs))
                goto err;
            res = result[x];
            if (last != res) {
                error("compare and swap doesn't match (expected %llx vs. %llx)",
                        (long long)last, (long long)res);
                goto err;
            }
            if (last)
                    last++;
            else
                    last = 0x0123456789abcdefULL;
            next = cur + 1;
            if (!ib_post_compare_swap(&ibdev, x, x*sizeof(uint64_t),
                                                            cur, next))
                goto err;
            cur = next;
        }
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
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
    IBDEV ibdev;
    uint64_t *result;
    uint64_t last = 0;
    int i;
    int size;

    ib_params_atomics();
    if (!ib_open(&ibdev, IBV_QPT_RC, NCQE, 0))
        goto err;
    size = Req.rd_atomic * sizeof(uint64_t);
    setv_u32(L_MSG_SIZE, size);
    setv_u32(R_MSG_SIZE, size);
    ib_mralloc(&ibdev, size);
    if (!ib_init(&ibdev))
        goto err;
    if (!synchronize())
        goto err;
    for (i = 0; i < Req.rd_atomic; ++i) {
        if (!ib_post_fetch_add(&ibdev, i, i*sizeof(uint64_t), 1))
            goto err;
    }
    result = (uint64_t *) ibdev.buffer;
    while (!Finished) {
        struct ibv_wc wc[NCQE];
        int n = ib_poll(&ibdev, wc, cardof(wc));
        uint64_t res;

        if (Finished)
            break;
        if (n < 0)
            goto err;
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        for (i = 0; i < n; ++i) {
            int x = wc[i].wr_id;
            int status = wc[i].status;
            if (status == IBV_WC_SUCCESS) {
                LStat.rem_r.no_bytes += sizeof(uint64_t);
                LStat.rem_r.no_msgs++;
            } else if (!do_error(status, &LStat.s.no_errs))
                goto err;
            res = result[x];
            if (last != res) {
                error("fetch and add doesn't match (expected %llx vs. %llx)",
                        (long long)last, (long long)res);
                goto err;
            }
            last++;
            if (!ib_post_fetch_add(&ibdev, x, x*sizeof(uint64_t), 1))
                goto err;
        }
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
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
    int r;
    IBDEV ibdev;

    ib_params_atomics();
    if (!ib_open(&ibdev, IBV_QPT_RC, NCQE, 0))
        goto err;
    setv_u32(L_MSG_SIZE, sizeof(uint64_t));
    setv_u32(R_MSG_SIZE, sizeof(uint64_t));
    ib_mralloc(&ibdev, sizeof(uint64_t));
    if (!ib_init(&ibdev))
        goto err;
    if (!synchronize())
        goto err;
    for (i = 0; i < Req.rd_atomic; ++i) {
        r = (atomic == FETCH_ADD)
          ? ib_post_fetch_add(&ibdev, 0, 0, 0)
          : ib_post_compare_swap(&ibdev, 0, 0, 0, 0);
        if (!r)
            goto err;
    }
    while (!Finished) {
        struct ibv_wc wc[NCQE];
        int n = ib_poll(&ibdev, wc, cardof(wc));
        if (Finished)
            break;
        if (n < 0)
            goto err;
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        for (i = 0; i < n; ++i) {
            int status = wc[i].status;
            if (status == IBV_WC_SUCCESS) {
                LStat.rem_r.no_bytes += sizeof(uint64_t);
                LStat.rem_r.no_msgs++;
            } else if (!do_error(status, &LStat.s.no_errs))
                goto err;
            r = (atomic == FETCH_ADD)
              ? ib_post_fetch_add(&ibdev, 0, 0, 0)
              : ib_post_compare_swap(&ibdev, 0, 0, 0, 0);
            if (!r)
                goto err;
        }
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
    show_results(MSG_RATE);
}


/*
 * Measure IB bandwidth (client side).
 */
static void
ib_client_bw(int transport)
{
    IBDEV ibdev;

    long sent = 0;
    if (!ib_open(&ibdev, transport, NCQE, 0))
        goto err;
    if (!ib_init(&ibdev))
        goto err;
    if (!synchronize())
        goto err;
    if (!ib_post_send(&ibdev, left_to_send(&sent, NCQE)))
        goto err;
    sent = NCQE;
    while (!Finished) {
        int i;
        struct ibv_wc wc[NCQE];

        int n = ib_poll(&ibdev, wc, cardof(wc));
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        if (n < 0)
            goto err;
        if (Finished)
            break;
        for (i = 0; i < n; ++i) {
            int id = wc[i].wr_id;
            int status = wc[i].status;
            if (id != WRID_SEND)
                debug("bad WR ID %d", id);
            else if (status != IBV_WC_SUCCESS)
                if (!do_error(status, &LStat.s.no_errs))
                    goto err;
        }
        if (Req.no_msgs) {
            if (LStat.s.no_msgs + LStat.s.no_errs >= Req.no_msgs)
                break;
            n = left_to_send(&sent, n);
        }
        if (!ib_post_send(&ibdev, n))
            goto err;
        sent += n;
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
}


/*
 * Default action for the server is to post receive buffers and whenever it
 * gets a completion entry, compute statistics and post more buffers.
 */
static void
ib_server_def(int transport)
{
    IBDEV ibdev;

    if (!ib_open(&ibdev, transport, 0, NCQE))
        return;
    if (!ib_init(&ibdev))
        goto err;
    if (!ib_post_recv(&ibdev, NCQE))
        goto err;
    if (!synchronize())
        goto err;
    while (!Finished) {
        int i;
        struct ibv_wc wc[NCQE];
        int n = ib_poll(&ibdev, wc, cardof(wc));
        if (Finished)
            break;
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        if (n < 0)
            goto err;
        for (i = 0; i < n; ++i) {
            int status = wc[i].status;
            if (status == IBV_WC_SUCCESS) {
                LStat.r.no_bytes += Req.msg_size;
                LStat.r.no_msgs++;
                if (Req.access_recv)
                    touch_data(ibdev.buffer, Req.msg_size);
            } else if (!do_error(status, &LStat.r.no_errs))
                goto err;
        }
        if (Req.no_msgs)
            if (LStat.r.no_msgs + LStat.r.no_errs >= Req.no_msgs)
                break;
        if (!ib_post_recv(&ibdev, n))
            goto err;
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
}


/*
 * Measure bi-directional IB bandwidth.
 */
static void
ib_bi_bw(int transport)
{
    IBDEV ibdev;

    if (!ib_open(&ibdev, transport, NCQE, NCQE))
        goto err;
    if (!ib_init(&ibdev))
        goto err;
    if (!ib_post_recv(&ibdev, NCQE))
        goto err;
    if (!synchronize())
        goto err;
    if (!ib_post_send(&ibdev, NCQE))
        goto err;
    while (!Finished) {
        int i;
        struct ibv_wc wc[NCQE];
        int noSend = 0;
        int noRecv = 0;
        int n = ib_poll(&ibdev, wc, cardof(wc));
        if (Finished)
            break;
        if (n > LStat.max_cqes)
            LStat.max_cqes = n;
        if (n < 0)
            goto err;
        for (i = 0; i < n; ++i) {
            int id = wc[i].wr_id;
            int status = wc[i].status;
            switch (id) {
            case WRID_SEND:
                if (status != IBV_WC_SUCCESS)
                    if (!do_error(status, &LStat.s.no_errs))
                        goto err;
                ++noSend;
                break;
            case WRID_RECV:
                if (status == IBV_WC_SUCCESS) {
                    LStat.r.no_bytes += Req.msg_size;
                    LStat.r.no_msgs++;
                    if (Req.access_recv)
                        touch_data(ibdev.buffer, Req.msg_size);
                } else if (!do_error(status, &LStat.r.no_errs))
                    goto err;
                ++noRecv;
                break;
            default:
                debug("bad WR ID %d", id);
            }
        }
        if (noRecv)
            if (!ib_post_recv(&ibdev, noRecv))
                goto err;
        if (noSend)
            if (!ib_post_send(&ibdev, noSend))
                goto err;
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
}


/*
 * Measure ping-pong latency (client and server side).
 */
static void
ib_pp_lat(int transport, IOMODE iomode)
{
    IBDEV ibdev;

    if (!ib_open(&ibdev, transport, 1, 1))
        goto err;
    if (!ib_init(&ibdev))
        goto err;
    ib_pp_lat_loop(&ibdev, iomode);
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
    if (is_client())
        show_results(LATENCY);
}


/*
 * Loop sending packets back and forth to measure ping-pong latency.
 */
static void
ib_pp_lat_loop(IBDEV *ibdev, IOMODE iomode)
{
    int done = 1;
    if (!ib_post_recv(ibdev, 1))
        return;
    if (!synchronize())
        return;
    if (is_client()) {
        if (iomode == IO_SR) {
            if (!ib_post_send(ibdev, 1))
                return;
        } else {
            if (!ib_post_rdma(ibdev, IBV_WR_RDMA_WRITE_WITH_IMM, 1))
                return;
        }
        done = 0;
    }

    while (!Finished) {
        int i;
        struct ibv_wc wc[2];
        int n = ib_poll(ibdev, wc, cardof(wc));
        if (Finished)
            break;
        if (n < 0)
            return;
        for (i = 0; i < n; ++i) {
            int id = wc[i].wr_id;
            int status = wc[i].status;
            switch (id) {
            case WRID_SEND:
            case WRID_RDMA:
                if (status != IBV_WC_SUCCESS)
                    if (!do_error(status, &LStat.s.no_errs))
                        return;
                done |= 1;
                continue;
            case WRID_RECV:
                if (status == IBV_WC_SUCCESS) {
                    LStat.r.no_bytes += Req.msg_size;
                    LStat.r.no_msgs++;
                    if (!ib_post_recv(ibdev, 1))
                        return;
                } else if (!do_error(status, &LStat.r.no_errs))
                    return;
                done |= 2;
                continue;
            default:
                debug("bad WR ID %d", id);
                continue;
            }
            break;
        }
        if (done == 3) {
            if (iomode == IO_SR) {
                if (!ib_post_send(ibdev, 1))
                    return;
            } else {
                if (!ib_post_rdma(ibdev, IBV_WR_RDMA_WRITE_WITH_IMM, 1))
                    return;
            }
            done = 0;
        }
    }
    Successful = 1;
}


/*
 * Loop sending packets back and forth using RDMA Write and polling to measure
 * latency.  Note that if we increase the number of entries of wc to be NCQE,
 * on the PS HCA, the latency is much longer.
 */
static void
ib_rdma_write_poll_lat(int transport)
{
    IBDEV ibdev;
    volatile char *p;
    volatile char *q;
    int send  = is_client() ? 1 : 0;
    int locID = send;
    int remID = !locID;

    if (!ib_open(&ibdev, transport, NCQE, 0))
        goto err;
    if (!ib_init(&ibdev))
        goto err;
    if (!synchronize())
        goto err;
    p = &ibdev.buffer[0];
    q = &ibdev.buffer[Req.msg_size-1];
    while (!Finished) {
        *p = locID;
        *q = locID;
        if (send) {
            int i;
            int n;
            struct ibv_wc wc[2];

            if (!ib_post_rdma(&ibdev, IBV_WR_RDMA_WRITE, 1))
                goto err;
            if (Finished)
                break;
            n = ibv_poll_cq(ibdev.cq, cardof(wc), wc);
            if (n < 0) {
                syserror("CQ poll failed");
                goto err;
            }
            for (i = 0; i < n; ++i) {
                int id = wc[i].wr_id;
                int status = wc[i].status;
                if (id != WRID_RDMA)
                    debug("bad WR ID %d", id);
                else if (status != IBV_WC_SUCCESS) {
                    if (!do_error(status, &LStat.s.no_errs))
                        goto err;
                }
            }
        }
        while (!Finished)
            if (*p == remID && *q == remID)
                break;
        LStat.r.no_bytes += Req.msg_size;
        LStat.r.no_msgs++;
        send = 1;
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
}


/*
 * Measure RDMA Read latency (client side).
 */
static void
ib_client_rdma_read_lat(int transport)
{
    IBDEV ibdev;

    if (!ib_open(&ibdev, transport, 1, 0))
        goto err;
    if (!ib_init(&ibdev))
        goto err;
    if (!synchronize())
        goto err;
    if (!ib_post_rdma(&ibdev, IBV_WR_RDMA_READ, 1))
        goto err;
    while (!Finished) {
        struct ibv_wc wc;
        int n = ib_poll(&ibdev, &wc, 1);
        if (n < 0)
            goto err;
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
        } else if (!do_error(wc.status, &LStat.s.no_errs))
            goto err;
        if (!ib_post_rdma(&ibdev, IBV_WR_RDMA_READ, 1))
            goto err;
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
    show_results(LATENCY);
}


/*
 * Measure RDMA bandwidth (client side).
 */
static void
ib_client_rdma_bw(int transport, OPCODE opcode)
{
    IBDEV ibdev;

    if (!ib_open(&ibdev, transport, NCQE, 0))
        goto err;
    if (!ib_init(&ibdev))
        goto err;
    if (!synchronize())
        goto err;
    if (!ib_post_rdma(&ibdev, opcode, NCQE))
        goto err;
    while (!Finished) {
        int i;
        struct ibv_wc wc[NCQE];
        int n = ib_poll(&ibdev, wc, cardof(wc));
        if (Finished)
            break;
        if (n < 0)
            goto err;
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
                }
            } else if (!do_error(status, &LStat.s.no_errs))
                goto err;
        }
        if (!ib_post_rdma(&ibdev, opcode, n))
            goto err;
    }
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
}


/*
 * Server just waits and lets driver take care of any requests.
 */
static void
ib_server_nop(int transport)
{
    IBDEV ibdev;

    /* workaround: Size of RQ should be 0; bug in Mellanox driver */
    if (!ib_open(&ibdev, transport, 0, 1))
        goto err;
    if (!ib_init(&ibdev))
        goto err;
    if (!synchronize())
        goto err;
    while (!Finished)
        pause();
    Successful = 1;
err:
    stop_timing();
    exchange_results();
    ib_close(&ibdev);
}


/*
 * Set default IB parameters for tests that use messages.
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
    par_use(L_RATE);
    par_use(R_RATE);
    if (use_poll_mode) {
        par_use(L_POLL_MODE);
        par_use(R_POLL_MODE);
    }
    opt_check();
}


/*
 * Set default IB parameters for tests that use atomics.
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
    par_use(L_RATE);
    par_use(R_RATE);
    par_use(L_RD_ATOMIC);
    par_use(R_RD_ATOMIC);
    opt_check();

    setv_u32(L_MSG_SIZE, 0);
}


/*
 * IB initialization.
 */
static int
ib_init(IBDEV *ibdev)
{
    IBCON ibcon;

    if (is_client()) {
        client_send_request();
        enc_init(&ibcon);
        enc_ibcon(&ibdev->lcon);
        if (!send_mesg(&ibcon, sizeof(ibcon), "IB connection"))
            return 0;
        if (!recv_mesg(&ibcon, sizeof(ibcon), "IB connection"))
            return 0;
        dec_init(&ibcon);
        dec_ibcon(&ibdev->rcon);
    } else {
        if (!recv_mesg(&ibcon, sizeof(ibcon), "IB connection"))
            return 0;
        dec_init(&ibcon);
        dec_ibcon(&ibdev->rcon);
        enc_init(&ibcon);
        enc_ibcon(&ibdev->lcon);
        if (!send_mesg(&ibcon, sizeof(ibcon), "IB connection"))
            return 0;
    }
    if (!ib_prepare(ibdev))
        return 0;
    ib_debug_info(ibdev);
    return 1;
}


/*
 * Show debugging information.
 */
static void
ib_debug_info(IBDEV *ibdev)
{
    debug("L: lid=%04x qpn=%06x psn=%06x rkey=%08x vaddr=%010x",
                    ibdev->lcon.lid, ibdev->lcon.qpn, ibdev->lcon.psn,
                                    ibdev->lcon.rkey, ibdev->lcon.vaddr);
    debug("R: lid=%04x qpn=%06x psn=%06x rkey=%08x vaddr=%010x",
                    ibdev->rcon.lid, ibdev->rcon.qpn, ibdev->rcon.psn,
                                    ibdev->rcon.rkey, ibdev->rcon.vaddr);
}


/*
 * Open a RDMA device.
 */
static int
ib_open(IBDEV *ibdev, int trans, int maxSendWR, int maxRecvWR)
{
    /* Clear structure */
    memset(ibdev, 0, sizeof(*ibdev));

    /* Check and set MTU */
    {
        int mtu = Req.mtu_size;
        if (mtu == 256)
            ibdev->mtu = IBV_MTU_256;
        else if (mtu == 512)
            ibdev->mtu = IBV_MTU_512;
        else if (mtu == 1024)
            ibdev->mtu = IBV_MTU_1024;
        else if (mtu == 2048)
            ibdev->mtu = IBV_MTU_2048;
        else if (mtu == 4096)
            ibdev->mtu = IBV_MTU_4096;
        else
            error_die("Bad MTU: %d; must be 256/512/1K/2K/4K", mtu);
    }

    /* Set transport type */
    ibdev->trans = trans;

    /* Set port */
    {
        int port = 1;
        char *p = index(Req.id, ':');
        if (p) {
            *p++ = '\0';
            port = atoi(p);
            if (port < 1)
                error_die("Bad IB port: %d; must be at least 1", port);
        }
        ibdev->port = port;
    }

    /* Set rate */
    {
        RATES *q = Rates;
        RATES *r = q + cardof(Rates);

        for (;; ++q) {
            if (q >= r) {
                syserror("Bad rate: %s", Req.rate);
                goto err;
            }
            if (streq(Req.rate, q->name)) {
                ibdev->rate = q->rate;
                break;
            }
        }
    }

    /* Determine device and open */
    {
        struct ibv_device *device;
        char *name = Req.id[0] ? Req.id : 0;

        ibdev->devlist = ibv_get_device_list(0);
        if (!ibdev->devlist) {
            syserror("Failed to find any IB devices");
            goto err;
        }
        if (!name)
            device = *ibdev->devlist;
        else {
            struct ibv_device **d = ibdev->devlist;
            while ((device = *d++))
                if (streq(ibv_get_device_name(device), name))
                    break;
        }
        if (!device) {
            syserror("Failed to find IB device");
            goto err;
        }
        ibdev->context = ibv_open_device(device);
        if (!ibdev->context) {
            syserror("Failed to open device %s", ibv_get_device_name(device));
            goto err;
        }
    }

    /* Allocate completion channel */
    ibdev->channel = ibv_create_comp_channel(ibdev->context);
    if (!ibdev->channel) {
        syserror("Failed to create completion channel");
        goto err;
    }

    /* Allocate protection domain */
    ibdev->pd = ibv_alloc_pd(ibdev->context);
    if (!ibdev->pd) {
        syserror("Failed to allocate protection domain");
        goto err;
    }

    /* Allocate message buffer and memory region */
    {
        int bufSize = Req.msg_size;
        int pageSize = sysconf(_SC_PAGESIZE);
        if (trans == IBV_QPT_UD)
            bufSize += GRH_SIZE;
        if (bufSize == 0)
            bufSize = 1;
        if (posix_memalign((void **)&ibdev->buffer, pageSize, bufSize) != 0) {
            syserror("Failed to allocate memory");
            goto err;
        }
        memset(ibdev->buffer, 0, bufSize);
        int flags = IBV_ACCESS_LOCAL_WRITE  |
                    IBV_ACCESS_REMOTE_READ  |
                    IBV_ACCESS_REMOTE_WRITE |
                    IBV_ACCESS_REMOTE_ATOMIC;
        ibdev->mr = ibv_reg_mr(ibdev->pd, ibdev->buffer, bufSize, flags);
        if (!ibdev->mr) {
            syserror("Failed to allocate memory region");
            goto err;
        }
    }

    /* Create completion queue */
    ibdev->cq = ibv_create_cq(ibdev->context,
                              maxSendWR+maxRecvWR, 0, ibdev->channel, 0);
    if (!ibdev->cq) {
        syserror("Failed to create completion queue");
        goto err;
    }

    /* Create queue pair */
    {
        struct ibv_qp_init_attr attr ={
            .send_cq = ibdev->cq,
            .recv_cq = ibdev->cq,
            .cap     ={
                .max_send_wr        = maxSendWR,
                .max_recv_wr        = maxRecvWR,
                .max_send_sge       = 1,
                .max_recv_sge       = 1,
                .max_inline_data    = 0,
            },
            .qp_type = ibdev->trans,
        };
        ibdev->qp = ibv_create_qp(ibdev->pd, &attr);
        if (!ibdev->qp) {
            syserror("Failed to create QP");
            goto err;
        }
    }

    /* Modify queue pair to INIT state */
    {
        struct ibv_qp_attr attr ={
            .qp_state       = IBV_QPS_INIT,
            .pkey_index     = 0,
            .port_num       = ibdev->port
        };
        int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT;

        if (ibdev->trans == IBV_QPT_UD) {
            flags |= IBV_QP_QKEY;
            attr.qkey = QKEY;
        } else if (ibdev->trans == IBV_QPT_RC) {
            flags |= IBV_QP_ACCESS_FLAGS;
            attr.qp_access_flags =
                IBV_ACCESS_REMOTE_READ  |
                IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_ATOMIC;
        } else if (ibdev->trans == IBV_QPT_UC) {
            flags |= IBV_QP_ACCESS_FLAGS;
            attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
        }
        if (ibv_modify_qp(ibdev->qp, &attr, flags) != SUCCESS0) {
            syserror("Failed to modify QP to INIT state");
            goto err;
        }
    }

    /* Get QP attributes */
    {
        struct ibv_qp_attr qp_attr;
        struct ibv_qp_init_attr qp_init_attr;

        if (ibv_query_qp(ibdev->qp, &qp_attr, 0, &qp_init_attr) != SUCCESS0) {
            syserror("Query QP failed");
            goto err;
        }
        ibdev->maxinline = qp_attr.cap.max_inline_data;
    }

    /* Get device properties */
    {
        struct ibv_device_attr dev_attr;

        if (ibv_query_device(ibdev->context, &dev_attr) != SUCCESS0) {
            syserror("Query device failed");
            goto err;
        }
        if (Req.rd_atomic == 0)
            Req.rd_atomic = dev_attr.max_qp_rd_atom;
        else if (Req.rd_atomic > dev_attr.max_qp_rd_atom)
            error("Device only supports %d (< %d) RDMA reads or atomic ops",
                  dev_attr.max_qp_rd_atom, Req.rd_atomic);
    }

    /* Set up local context */
    {
        struct ibv_port_attr port_attr;

        int stat = ibv_query_port(ibdev->context, ibdev->port, &port_attr);
        if (stat != SUCCESS0) {
            syserror("Query port failed");
            goto err;
        }
        srand48(getpid()*time(0));

        ibdev->lcon.lid   = port_attr.lid;
        ibdev->lcon.qpn   = ibdev->qp->qp_num;
        ibdev->lcon.psn   = lrand48() & 0xffffff;
        ibdev->lcon.rkey  = 0;
        ibdev->lcon.vaddr = 0;
    }

    /* Allocate memory region */
    if (!ib_mralloc(ibdev, Req.msg_size))
        goto err;
    return 1;

err:
    ib_close(ibdev);
    return 0;
}


/*
 * Allocate a memory region.
 */
static int
ib_mralloc(IBDEV *ibdev, int size)
{
    int pageSize;

    if (size == 0)
        return 1;
    if (ibdev->trans == IBV_QPT_UD)
        size += GRH_SIZE;
    pageSize = sysconf(_SC_PAGESIZE);
    if (posix_memalign((void **)&ibdev->buffer, pageSize, size) != 0) {
        syserror("Failed to allocate memory");
        goto err;
    }
    memset(ibdev->buffer, 0, size);
    int flags = IBV_ACCESS_LOCAL_WRITE  |
                IBV_ACCESS_REMOTE_READ  |
                IBV_ACCESS_REMOTE_WRITE |
                IBV_ACCESS_REMOTE_ATOMIC;
    ibdev->mr = ibv_reg_mr(ibdev->pd, ibdev->buffer, size, flags);
    if (!ibdev->mr) {
        syserror("Failed to allocate memory region");
        goto err;
    }

    ibdev->lcon.rkey  = ibdev->mr->rkey;
    ibdev->lcon.vaddr = (unsigned long)ibdev->buffer;
    return 1;

err:
    if (ibdev->buffer) {
        free(ibdev->buffer);
        ibdev->buffer = 0;
    }
    return 0;
}


/*
 * Prepare the IB device for receiving and sending.
 */
static int
ib_prepare(IBDEV *ibdev)
{
    int flags;
    struct ibv_qp_attr rtr_attr ={
        .qp_state           = IBV_QPS_RTR,
        .path_mtu           = ibdev->mtu,
        .dest_qp_num        = ibdev->rcon.qpn,
        .rq_psn             = ibdev->rcon.psn,
        .min_rnr_timer      = RNR_TIMER,
        .max_dest_rd_atomic = Req.rd_atomic,
        .ah_attr            = {
            .dlid           = ibdev->rcon.lid,
            .port_num       = ibdev->port,
            .static_rate    = ibdev->rate
        }
    };
    struct ibv_qp_attr rts_attr ={
        .qp_state       = IBV_QPS_RTS,
        .timeout        = TIMEOUT,
        .retry_cnt      = RETRY_CNT,
        .rnr_retry      = RNR_RETRY,
        .sq_psn         = ibdev->lcon.psn,
        .max_rd_atomic  = Req.rd_atomic
    };
    struct ibv_ah_attr ah_attr ={
        .dlid           = ibdev->rcon.lid,
        .port_num       = ibdev->port,
        .static_rate    = ibdev->rate
    };

    if (ibdev->trans == IBV_QPT_UD) {
        /* Modify queue pair to RTR */
        flags = IBV_QP_STATE;
        if (ibv_modify_qp(ibdev->qp, &rtr_attr, flags) != SUCCESS0)
            return syserror("Failed to modify QP to RTR");

        /* Modify queue pair to RTS */
        flags = IBV_QP_STATE | IBV_QP_SQ_PSN;
        if (ibv_modify_qp(ibdev->qp, &rts_attr, flags) != SUCCESS0)
            return syserror("Failed to modify QP to RTS");

        /* Create address handle */
        ibdev->ah = ibv_create_ah(ibdev->pd, &ah_attr);
        if (!ibdev->ah)
            return syserror("Failed to create address handle");
    } else if (ibdev->trans == IBV_QPT_RC) {
        /* Modify queue pair to RTR */
        flags = IBV_QP_STATE              |
                IBV_QP_AV                 |
                IBV_QP_PATH_MTU           |
                IBV_QP_DEST_QPN           |
                IBV_QP_RQ_PSN             |
                IBV_QP_MAX_DEST_RD_ATOMIC |
                IBV_QP_MIN_RNR_TIMER;
        if (ibv_modify_qp(ibdev->qp, &rtr_attr, flags) != SUCCESS0)
            return syserror("Failed to modify QP to RTR");

        /* Modify queue pair to RTS */
        flags = IBV_QP_STATE     |
                IBV_QP_TIMEOUT   |
                IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY |
                IBV_QP_SQ_PSN    |
                IBV_QP_MAX_QP_RD_ATOMIC;
        if (ibv_modify_qp(ibdev->qp, &rts_attr, flags) != SUCCESS0)
            return syserror("Failed to modify QP to RTS");
    } else if (ibdev->trans == IBV_QPT_UC) {
        /* Modify queue pair to RTR */
        flags = IBV_QP_STATE    |
                IBV_QP_AV       |
                IBV_QP_PATH_MTU |
                IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN;
        if (ibv_modify_qp(ibdev->qp, &rtr_attr, flags) != SUCCESS0)
            return syserror("Failed to modify QP to RTR");

        /* Modify queue pair to RTS */
        flags = IBV_QP_STATE |
                IBV_QP_SQ_PSN;
        if (ibv_modify_qp(ibdev->qp, &rts_attr, flags) != SUCCESS0)
            return syserror("Failed to modify QP to RTS");
    }
    if (!Req.poll_mode) {
        if (ibv_req_notify_cq(ibdev->cq, 0) != SUCCESS0)
            return syserror("Failed to request CQ notification");
    }
    return 1;
}


/*
 * Close a RDMA device.  We ust destroy the CQ before the QP otherwise the
 * ibv_destroy_qp call might hang.
 */
static void
ib_close(IBDEV *ibdev)
{
    if (ibdev->ah)
        ibv_destroy_ah(ibdev->ah);
    if (ibdev->cq)
        ibv_destroy_cq(ibdev->cq);
    if (ibdev->qp)
        ibv_destroy_qp(ibdev->qp);
    if (ibdev->mr)
        ibv_dereg_mr(ibdev->mr);
    if (ibdev->pd)
        ibv_dealloc_pd(ibdev->pd);
    if (ibdev->channel)
        ibv_destroy_comp_channel(ibdev->channel);
    if (ibdev->context)
        ibv_close_device(ibdev->context);
    if (ibdev->buffer)
        free(ibdev->buffer);
    if (ibdev->devlist)
        free(ibdev->devlist);
    memset(ibdev, 0, sizeof(*ibdev));
}


/*
 * Post a compare and swap request.
 */
static int
ib_post_compare_swap(IBDEV *ibdev,
                     int wrid, int offset, uint64_t compare, uint64_t swap)
{
    struct ibv_sge sge ={
        .addr   = (uintptr_t)ibdev->buffer + offset,
        .length = sizeof(uint64_t),
        .lkey   = ibdev->mr->lkey
    };
    struct ibv_send_wr wr ={
        .wr_id      = wrid,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_ATOMIC_CMP_AND_SWP,
        .send_flags = IBV_SEND_SIGNALED,
        .wr = {
            .atomic = {
                .remote_addr    = ibdev->rcon.vaddr,
                .rkey           = ibdev->rcon.rkey,
                .compare_add    = compare,
                .swap           = swap
            }
        }
    };
    struct ibv_send_wr *badWR;

    errno = 0;
    if (ibv_post_send(ibdev->qp, &wr, &badWR) != SUCCESS0) {
        if (Finished && errno == EINTR)
            return 1;
        return syserror("Failed to post compare and swap");
    }

    LStat.s.no_bytes += sizeof(uint64_t);
    LStat.s.no_msgs++;
    return 1;
}


/*
 * Post a fetch and add request.
 */
static int
ib_post_fetch_add(IBDEV *ibdev, int wrid, int offset, uint64_t add)
{
    struct ibv_sge sge ={
        .addr   = (uintptr_t) ibdev->buffer + offset,
        .length = sizeof(uint64_t),
        .lkey   = ibdev->mr->lkey
    };
    struct ibv_send_wr wr ={
        .wr_id      = wrid,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_ATOMIC_FETCH_AND_ADD,
        .send_flags = IBV_SEND_SIGNALED,
        .wr = {
            .atomic = {
                .remote_addr    = ibdev->rcon.vaddr,
                .rkey           = ibdev->rcon.rkey,
                .compare_add    = add
            }
        }
    };
    struct ibv_send_wr *badWR;

    errno = 0;
    if (ibv_post_send(ibdev->qp, &wr, &badWR) != SUCCESS0) {
        if (Finished && errno == EINTR)
            return 1;
        return syserror("Failed to post fetch and add");
    }

    LStat.s.no_bytes += sizeof(uint64_t);
    LStat.s.no_msgs++;
    return 1;
}


/*
 * Post n sends.
 */
static int
ib_post_send(IBDEV *ibdev, int n)
{
    struct ibv_sge sge ={
        .addr   = (uintptr_t) ibdev->buffer,
        .length = Req.msg_size,
        .lkey   = ibdev->mr->lkey
    };
    struct ibv_send_wr wr ={
        .wr_id      = WRID_SEND,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
    };
    struct ibv_send_wr *badWR;

    if (ibdev->trans == IBV_QPT_UD) {
        wr.wr.ud.ah          = ibdev->ah;
        wr.wr.ud.remote_qpn  = ibdev->rcon.qpn;
        wr.wr.ud.remote_qkey = QKEY;
    }
    if (Req.msg_size <= ibdev->maxinline)
        wr.send_flags |= IBV_SEND_INLINE;
    errno = 0;
    while (n-- > 0) {
        if (ibv_post_send(ibdev->qp, &wr, &badWR) != SUCCESS0) {
            if (Finished && errno == EINTR)
                return 1;
            return syserror("Failed to post send");
        }
        LStat.s.no_bytes += Req.msg_size;
        LStat.s.no_msgs++;
    }

    return 1;
}


/*
 * Post n receives.
 */
static int
ib_post_recv(IBDEV *ibdev, int n)
{
    struct ibv_sge sge ={
        .addr   = (uintptr_t) ibdev->buffer,
        .length = Req.msg_size,
        .lkey   = ibdev->mr->lkey
    };
    struct ibv_recv_wr wr ={
        .wr_id      = WRID_RECV,
        .sg_list    = &sge,
        .num_sge    = 1,
    };
    struct ibv_recv_wr *badWR;

    if (ibdev->trans == IBV_QPT_UD)
        sge.length += GRH_SIZE;

    errno = 0;
    while (n-- > 0) {
        if (ibv_post_recv(ibdev->qp, &wr, &badWR) != SUCCESS0) {
            if (Finished && errno == EINTR)
                return 1;
            return syserror("Failed to post receive");
        }
    }
    return 1;
}


/*
 * Post n RDMA requests.
 */
static int
ib_post_rdma(IBDEV *ibdev, OPCODE opcode, int n)
{
    struct ibv_sge sge ={
        .addr   = (uintptr_t) ibdev->buffer,
        .length = Req.msg_size,
        .lkey   = ibdev->mr->lkey
    };
    struct ibv_send_wr wr ={
        .wr_id      = WRID_RDMA,
        .sg_list    = &sge,
        .num_sge    = 1,
        .opcode     = opcode,
        .send_flags = IBV_SEND_SIGNALED,
        .wr = {
            .rdma = {
                .remote_addr = ibdev->rcon.vaddr,
                .rkey        = ibdev->rcon.rkey
            }
        }
    };
    struct ibv_send_wr *badWR;

    if (opcode != IBV_WR_RDMA_READ && Req.msg_size <= ibdev->maxinline)
        wr.send_flags |= IBV_SEND_INLINE;
    errno = 0;
    while (n--) {
        if (ibv_post_send(ibdev->qp, &wr, &badWR) != SUCCESS0) {
            if (Finished && errno == EINTR)
                return 1;
            return syserror("Failed to post %s", opcode_name(wr.opcode));
        }
        if (opcode != IBV_WR_RDMA_READ) {
            LStat.s.no_bytes += Req.msg_size;
            LStat.s.no_msgs++;
        }
    }
    return 1;
}


/*
 * Poll the completion queue.
 */
static int
ib_poll(IBDEV *ibdev, struct ibv_wc *wc, int nwc)
{
    int n;
    char *msg;

    if (!Req.poll_mode && !Finished) {
        void *ectx;
        struct ibv_cq *ecq;

        if (ibv_get_cq_event(ibdev->channel, &ecq, &ectx) != SUCCESS0)
            {msg = "failed to get CQ event"; goto err;}
        if (ecq != ibdev->cq)
            {msg = "CQ event for unknown CQ"; goto err;}
        if (ibv_req_notify_cq(ibdev->cq, 0) != SUCCESS0)
            {msg = "failed to request CQ notification"; goto err;}
    }
    n = ibv_poll_cq(ibdev->cq, nwc, wc);
    if (n < 0)
        {msg = "CQ poll failed"; goto err;}
    return n;

err:
    if (Finished && errno == EINTR)
        return 0;
    syserror(msg);
    return -1;
}


/*
 * Encode a IBCON structure into a data stream.
 */
static void
enc_ibcon(IBCON *host)
{
    enc_int(host->lid,   sizeof(host->lid));
    enc_int(host->qpn,   sizeof(host->qpn));
    enc_int(host->psn,   sizeof(host->psn));
    enc_int(host->rkey,  sizeof(host->rkey));
    enc_int(host->vaddr, sizeof(host->vaddr));
}


/*
 * Decode a IBCON structure from a data stream.
 */
static void
dec_ibcon(IBCON *host)
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
static int
do_error(int status, uint64_t *errors)
{
    ++*errors;
    cq_error(status);
    return 0;
}


/*
 * Print out a CQ error given a status.
 */
static void
cq_error(int status)
{
    int i;

    for (i = 0; i < cardof(CQErrors); ++i) {
        if (CQErrors[i].value == status) {
            error("%s failed: %s", TestName, CQErrors[i].name);
            return;
        }
    }
    error("%s failed: CQ error %d", TestName, status);
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
