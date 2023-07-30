/*
 * Copyright (c) 2005 Topspin Communications.  All rights reserved.
 * Copyright (c) 2006 Cisco Systems.  All rights reserved.
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


#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <stdlib.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <math.h>
#include <infiniband/verbs.h>

// ------------------ IMPORT TESTS ------------------
#include "tests.c"
// ------------------ IMPORT TESTS ------------------

#define WC_BATCH (1)
#define MAXIMUM_SIZE (1048576L)
#define BITS_4_KB (4096)
#define DATABASE_INITIAL_CAPACITY (4)
#define DATABASE_FACTOR_CAPACITY (2)
#define NUMBER_OF_CLIENTS (1)
#define MAXIMUM_HANDLE_REQUESTS_BUFFERS (5)
int MAXIMUM_HANDLE = MAXIMUM_HANDLE_REQUESTS_BUFFERS;

enum {
    PINGPONG_RECV_WRID = 1,
    PINGPONG_SEND_WRID = 2,
};

enum REQUEST {
    GET,
    SET
};

enum PROTOCOL {
    EAGER,
    RENDEZVOUS
};


struct Rendezvous {
    uint32_t rkey;
    void * remote_addr;
    unsigned long size;
};



struct packet {
    char value[BITS_4_KB];
    char response_value[BITS_4_KB];
    char key[BITS_4_KB];
    char * bigValue;
    enum REQUEST request_type;
    enum PROTOCOL protocol_type;
    struct Rendezvous rendezvous_get;
    struct Rendezvous rendezvous_set;
};


struct Database {
    struct packet ** map;
    int curr_size;
    int capacity;
};

static int page_size;
int argc_global;
char **argv_global;

struct pingpong_context {
    struct ibv_context		*context;
    struct ibv_comp_channel	*channel;
    struct ibv_pd		*pd;
    struct ibv_mr		*mr[MAXIMUM_HANDLE_REQUESTS_BUFFERS];
    struct ibv_cq		*cq;
    struct ibv_qp		*qp;
    void					*buf[MAXIMUM_HANDLE_REQUESTS_BUFFERS];
    unsigned long		size;
    int				rx_depth;
    int				routs;
    struct ibv_port_attr	portinfo;
    size_t currBuffer;
};

struct pingpong_dest {
    int lid;
    int qpn;
    int psn;
    union ibv_gid gid;
};

enum ibv_mtu pp_mtu_to_enum(int mtu)
{
    switch (mtu) {
        case 256:  return IBV_MTU_256;
        case 512:  return IBV_MTU_512;
        case 1024: return IBV_MTU_1024;
        case 2048: return IBV_MTU_2048;
        case 4096: return IBV_MTU_4096;
        default:   return -1;
    }
}

uint16_t pp_get_local_lid(struct ibv_context *context, int port)
{
    struct ibv_port_attr attr;

    if (ibv_query_port(context, port, &attr))
        return 0;

    return attr.lid;
}

int pp_get_port_info(struct ibv_context *context, int port,
                     struct ibv_port_attr *attr)
{
    return ibv_query_port(context, port, attr);
}

void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
    char tmp[9];
    uint32_t v32;
    int i;

    for (tmp[8] = 0, i = 0; i < 4; ++i) {
        memcpy(tmp, wgid + i * 8, 8);
        sscanf(tmp, "%x", &v32);
        *(uint32_t *)(&gid->raw[i * 4]) = ntohl(v32);
    }
}

void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
    int i;

    for (i = 0; i < 4; ++i)
        sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *)(gid->raw + i * 4)));
}

static int pp_connect_ctx(struct pingpong_context *ctx, int port, int my_psn,
                          enum ibv_mtu mtu, int sl,
                          struct pingpong_dest *dest, int sgid_idx)
{
    struct ibv_qp_attr attr = {
            .qp_state		= IBV_QPS_RTR,
            .path_mtu		= mtu,
            .dest_qp_num		= dest->qpn,
            .rq_psn			= dest->psn,
            .max_dest_rd_atomic	= 1,
            .min_rnr_timer		= 12,
            .ah_attr		= {
                    .is_global	= 0,
                    .dlid		= dest->lid,
                    .sl		= sl,
                    .src_path_bits	= 0,
                    .port_num	= port
            }
    };

    if (dest->gid.global.interface_id) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.dgid = dest->gid;
        attr.ah_attr.grh.sgid_index = sgid_idx;
    }
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE              |
                      IBV_QP_AV                 |
                      IBV_QP_PATH_MTU           |
                      IBV_QP_DEST_QPN           |
                      IBV_QP_RQ_PSN             |
                      IBV_QP_MAX_DEST_RD_ATOMIC |
                      IBV_QP_MIN_RNR_TIMER)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    attr.qp_state	    = IBV_QPS_RTS;
    attr.timeout	    = 14;
    attr.retry_cnt	    = 7;
    attr.rnr_retry	    = 7;
    attr.sq_psn	    = my_psn;
    attr.max_rd_atomic  = 1;
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE              |
                      IBV_QP_TIMEOUT            |
                      IBV_QP_RETRY_CNT          |
                      IBV_QP_RNR_RETRY          |
                      IBV_QP_SQ_PSN             |
                      IBV_QP_MAX_QP_RD_ATOMIC)) {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }

    return 0;
}

static struct pingpong_dest *pp_client_exch_dest(const char *servername, int port,
                                                 const struct pingpong_dest *my_dest)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
            .ai_family   = AF_INET,
            .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(servername, service, &hints, &res);

    if (n < 0) {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
        free(service);
        return NULL;
    }
    for (t = res; t; t = t->ai_next) {

        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0) {
            if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0) {
        fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        return NULL;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(sockfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        goto out;
    }

    if (read(sockfd, msg, sizeof msg) != sizeof msg) {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        goto out;
    }

    write(sockfd, "done", sizeof "done");

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    out:
    close(sockfd);
    return rem_dest;
}

static struct pingpong_dest *pp_server_exch_dest(struct pingpong_context *ctx,
                                                 int ib_port, enum ibv_mtu mtu,
                                                 int port, int sl,
                                                 const struct pingpong_dest *my_dest,
                                                 int sgid_idx)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
            .ai_flags    = AI_PASSIVE,
            .ai_family   = AF_INET,
            .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1, connfd;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(NULL, service, &hints, &res);

    if (n < 0) {
        fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next) {

        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0) {
            n = 1;

            setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

            if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0) {
        fprintf(stderr, "Couldn't listen to port %d\n", port);
        return NULL;
    }

    listen(sockfd, 1);

    connfd = accept(sockfd, NULL, 0);
    close(sockfd);
    if (connfd < 0) {
        fprintf(stderr, "accept() failed\n");
        return NULL;
    }

    n = read(connfd, msg, sizeof msg);
    if (n != sizeof msg) {
        perror("server read");
        fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
        goto out;
    }

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    if (pp_connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx)) {
        fprintf(stderr, "Couldn't connect to remote QP\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }


    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(connfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    read(connfd, msg, sizeof msg);

    out:
    close(connfd);
    return rem_dest;
}

#include <sys/param.h>

static struct pingpong_context *pp_init_ctx(struct ibv_device *ib_dev, int size,
                                            int rx_depth, int tx_depth, int port,
                                            int use_event, int is_server)
{
    struct pingpong_context *ctx;

    ctx = calloc(1, sizeof *ctx);
    if (!ctx)
        return NULL;

    ctx->size     = size;
    ctx->rx_depth = rx_depth;
    ctx->routs    = rx_depth;
    for (int i = 0; i < MAXIMUM_HANDLE_REQUESTS_BUFFERS; i++) {
        ctx->buf[i] = malloc(roundup(size, page_size));
        if (!ctx->buf[i]) {
            fprintf(stderr, "Couldn't allocate work buf.\n");
            return NULL;
        }
        memset(ctx->buf[i], 0x7b + is_server, size);
    }

    ctx->context = ibv_open_device(ib_dev);
    if (!ctx->context) {
        fprintf(stderr, "Couldn't get context for %s\n",
                ibv_get_device_name(ib_dev));
        return NULL;
    }

    if (use_event) {
        ctx->channel = ibv_create_comp_channel(ctx->context);
        if (!ctx->channel) {
            fprintf(stderr, "Couldn't create completion channel\n");
            return NULL;
        }
    } else
        ctx->channel = NULL;

    ctx->pd = ibv_alloc_pd(ctx->context);
    if (!ctx->pd) {
        fprintf(stderr, "Couldn't allocate PD\n");
        return NULL;
    }
    for (int i = 0; i < MAXIMUM_HANDLE_REQUESTS_BUFFERS; i++) {
        ctx->mr[i] = ibv_reg_mr(ctx->pd, ctx->buf[i], size, IBV_ACCESS_LOCAL_WRITE);
        if (!ctx->mr[i]) {
            fprintf(stderr, "Couldn't register MR\n");
            return NULL;
        }
    }



    ctx->cq = ibv_create_cq(ctx->context, rx_depth + tx_depth, NULL,
                            ctx->channel, 0);
    if (!ctx->cq) {
        fprintf(stderr, "Couldn't create CQ\n");
        return NULL;
    }

    {
        struct ibv_qp_init_attr attr = {
                .send_cq = ctx->cq,
                .recv_cq = ctx->cq,
                .cap     = {
                        .max_send_wr  = tx_depth,
                        .max_recv_wr  = rx_depth,
                        .max_send_sge = 1,
                        .max_recv_sge = 1
                },
                .qp_type = IBV_QPT_RC
        };

        ctx->qp = ibv_create_qp(ctx->pd, &attr);
        if (!ctx->qp)  {
            fprintf(stderr, "Couldn't create QP\n");
            return NULL;
        }
    }

    {
        struct ibv_qp_attr attr = {
                .qp_state        = IBV_QPS_INIT,
                .pkey_index      = 0,
                .port_num        = port,
                .qp_access_flags = IBV_ACCESS_REMOTE_READ |
                                   IBV_ACCESS_REMOTE_WRITE
        };

        if (ibv_modify_qp(ctx->qp, &attr,
                          IBV_QP_STATE              |
                          IBV_QP_PKEY_INDEX         |
                          IBV_QP_PORT               |
                          IBV_QP_ACCESS_FLAGS)) {
            fprintf(stderr, "Failed to modify QP to INIT\n");
            return NULL;
        }
    }

    return ctx;
}

int pp_close_ctx(struct pingpong_context *ctx)
{
    if (ibv_destroy_qp(ctx->qp)) {
        fprintf(stderr, "Couldn't destroy QP\n");
        return 1;
    }

    if (ibv_destroy_cq(ctx->cq)) {
        fprintf(stderr, "Couldn't destroy CQ\n");
        return 1;
    }
    for (int i = 0; i < MAXIMUM_HANDLE_REQUESTS_BUFFERS; i++) {
        if (ibv_dereg_mr(ctx->mr[i])) {
            fprintf(stderr, "Couldn't deregister MR\n");
            return 1;
        }
    }


    if (ibv_dealloc_pd(ctx->pd)) {
        fprintf(stderr, "Couldn't deallocate PD\n");
        return 1;
    }

    if (ctx->channel) {
        if (ibv_destroy_comp_channel(ctx->channel)) {
            fprintf(stderr, "Couldn't destroy completion channel\n");
            return 1;
        }
    }

    if (ibv_close_device(ctx->context)) {
        fprintf(stderr, "Couldn't release context\n");
        return 1;
    }

    free(ctx->buf);
    for (int i = 0; i < MAXIMUM_HANDLE_REQUESTS_BUFFERS; i++) {
        free(ctx->buf[i]);
    }
    free(ctx);

    return 0;
}

static int pp_post_recv(struct pingpong_context *ctx, int n)
{
    struct ibv_sge list = {
            .addr	= (uintptr_t) ctx->buf[ctx->currBuffer],
            .length = ctx->size,
            .lkey	= ctx->mr[ctx->currBuffer]->lkey
    };
    struct ibv_recv_wr wr = {
            .wr_id	    = PINGPONG_RECV_WRID,
            .sg_list    = &list,
            .num_sge    = 1,
            .next       = NULL
    };
    struct ibv_recv_wr *bad_wr;
    int i;

    for (i = 0; i < n; ++i)
        if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
            break;

    return i;
}

static int pp_post_send(struct pingpong_context *ctx, const char *local_ptr, void *remote_ptr, uint32_t remote_key,
                        enum ibv_wr_opcode opcode) {
    struct ibv_sge list = {
            .addr	= (uintptr_t) (local_ptr ? local_ptr : ctx->buf[ctx->currBuffer]),
            .length = ctx->size,
            .lkey	= ctx->mr[ctx->currBuffer]->lkey
    };
    struct ibv_send_wr *bad_wr, wr = {
            .wr_id	    = PINGPONG_SEND_WRID,
            .sg_list    = &list,
            .num_sge    = 1,
            .opcode     = opcode,
            .send_flags = IBV_SEND_SIGNALED,
            .next       = NULL
    };
    if (remote_ptr) {
        wr.wr.rdma.remote_addr = (uintptr_t) remote_ptr;
        wr.wr.rdma.rkey = remote_key;
    }
    int a = ibv_post_send(ctx->qp, &wr, &bad_wr);
    return a;
}

int pp_wait_completions(struct pingpong_context *ctx, int iters) {
    int rcnt = 0, scnt = 0;
    while (rcnt + scnt < iters) {
        struct ibv_wc wc[WC_BATCH];
        int ne, i;

        do {
            ne = ibv_poll_cq(ctx->cq, WC_BATCH, wc);
            if (ne < 0) {
                fprintf(stderr, "poll CQ failed %d\n", ne);
                return 1;
            }

        } while (ne < 1);
        for (i = 0; i < ne; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                        ibv_wc_status_str(wc[i].status),
                        wc[i].status, (int) wc[i].wr_id);
                return  1;
            }


            switch ((int) wc[i].wr_id) {
                case PINGPONG_SEND_WRID:
                    ++scnt;
                    break;

                case PINGPONG_RECV_WRID:
                    ++rcnt;
                    break;

                default:
                    fprintf(stderr, "Completion for unknown wr_id %d\n",
                            (int) wc[i].wr_id);
                    return 1;
            }
        }

    }
    return 0;
}

static void usage(const char *argv0)
{
    printf("Usage:\n");
    printf("  %s            start a server and wait for connection\n", argv0);
    printf("  %s <host>     connect to server at <host>\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
    printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
    printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
    printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
    printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
    printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
    printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
    printf("  -l, --sl=<sl>          service level value\n");
    printf("  -e, --events           sleep on CQ events (default poll)\n");
    printf("  -g, --gid-idx=<gid index> local port gid index\n");
}

int connect_main(char *servername, int argc, char *argv[], struct pingpong_context **save_ctx){
    struct ibv_device      **dev_list;
    struct ibv_device       *ib_dev;
    struct pingpong_context *ctx;
    struct pingpong_dest     my_dest;
    struct pingpong_dest    *rem_dest;
    char                    *ib_devname = NULL;
    int                      port = 4792;
    int                      ib_port = 1;
    enum ibv_mtu             mtu = IBV_MTU_2048;
    int                      rx_depth = 5000;
    int                      tx_depth = 5000;
    int                      iters = 50000;
    int                      use_event = 0;
    int                      size = MAXIMUM_SIZE;
    int                      sl = 0;
    int                      gidx = -1;
    char                     gid[33];

    srand48(getpid() * time(NULL));

    while (1) {
        int c;

        static struct option long_options[] = {
                { .name = "port",     .has_arg = 1, .val = 'p' },
                { .name = "ib-dev",   .has_arg = 1, .val = 'd' },
                { .name = "ib-port",  .has_arg = 1, .val = 'i' },
                { .name = "size",     .has_arg = 1, .val = 's' },
                { .name = "mtu",      .has_arg = 1, .val = 'm' },
                { .name = "rx-depth", .has_arg = 1, .val = 'r' },
                { .name = "iters",    .has_arg = 1, .val = 'n' },
                { .name = "sl",       .has_arg = 1, .val = 'l' },
                { .name = "events",   .has_arg = 0, .val = 'e' },
                { .name = "gid-idx",  .has_arg = 1, .val = 'g' },
                { 0 }
        };

        c = getopt_long(argc, argv, "p:d:i:s:m:r:n:l:eg:", long_options, NULL);
        if (c == -1)
            break;

        switch (c) {
            case 'p':
                port = strtol(optarg, NULL, 0);
                if (port < 0 || port > 65535) {
                    usage(argv[0]);
                    return 1;
                }
                break;

            case 'd':
                ib_devname = strdup(optarg);
                break;

            case 'i':
                ib_port = strtol(optarg, NULL, 0);
                if (ib_port < 0) {
                    usage(argv[0]);
                    return 1;
                }
                break;

            case 's':
                size = strtol(optarg, NULL, 0);
                break;

            case 'm':
                mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
                if (mtu < 0) {
                    usage(argv[0]);
                    return 1;
                }
                break;

            case 'r':
                rx_depth = strtol(optarg, NULL, 0);
                break;

            case 'n':
                iters = strtol(optarg, NULL, 0);
                break;

            case 'l':
                sl = strtol(optarg, NULL, 0);
                break;

            case 'e':
                ++use_event;
                break;

            case 'g':
                gidx = strtol(optarg, NULL, 0);
                break;

            default:
                usage(argv[0]);
                return 1;
        }
    }

//    if (optind == argc - 1)
//        servername = strdup(argv[optind]);
//    else if (optind < argc) {
//        usage(argv[0]);
//        return 1;
//    }

    page_size = sysconf(_SC_PAGESIZE);

    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        return 1;
    }

    if (!ib_devname) {
        ib_dev = *dev_list;
        if (!ib_dev) {
            fprintf(stderr, "No IB devices found\n");
            return 1;
        }
    } else {
        int i;
        for (i = 0; dev_list[i]; ++i)
            if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
                break;
        ib_dev = dev_list[i];
        if (!ib_dev) {
            fprintf(stderr, "IB device %s not found\n", ib_devname);
            return 1;
        }
    }

    ctx = pp_init_ctx(ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername);

    if (!ctx) return 1;

//    ctx->currBuffer = 0;
//    ctx->routs = pp_post_recv(ctx, ctx->rx_depth);
//    if (ctx->routs < ctx->rx_depth) {
//        fprintf(stderr, "Couldn't post receive (%d)\n", ctx->routs);
//        return 1;
//    }

    if (use_event)
        if (ibv_req_notify_cq(ctx->cq, 0)) {
            fprintf(stderr, "Couldn't request CQ notification\n");
            return 1;
        }


    if (pp_get_port_info(ctx->context, ib_port, &ctx->portinfo)) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    my_dest.lid = ctx->portinfo.lid;
    if (ctx->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !my_dest.lid) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    if (gidx >= 0) {
        if (ibv_query_gid(ctx->context, ib_port, gidx, &my_dest.gid)) {
            fprintf(stderr, "Could not get local gid for gid index %d\n", gidx);
            return 1;
        }
    } else
        memset(&my_dest.gid, 0, sizeof my_dest.gid);

    my_dest.qpn = ctx->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
//    printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
//           my_dest.lid, my_dest.qpn, my_dest.psn, gid);


    if (servername) {
        rem_dest = pp_client_exch_dest(servername, port, &my_dest);
    }
    else
        rem_dest = pp_server_exch_dest(ctx, ib_port, mtu, port, sl, &my_dest, gidx);

    if (!rem_dest)
        return 1;

    inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
//    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
//           rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

    if (servername)
        if (pp_connect_ctx(ctx, ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
            return 1;
    *save_ctx = ctx;
    ibv_free_device_list(dev_list);
    free(rem_dest);
    return 0;
}


int send_packet(struct pingpong_context *ctx) {
    ctx->size = sizeof (struct packet);
    if (pp_post_send(ctx, NULL, NULL, 0, IBV_WR_SEND)) {
        printf("%d%s", 1, "Error server send");
        return 1;
    }
    if(pp_wait_completions(ctx, 1)) {
        printf("%s", "Error completions");
        return 1;
    }
    return 0;
}

int receive_packet(struct pingpong_context *ctx) {
    ctx->size = sizeof (struct packet);
    if (pp_post_recv(ctx, 1) != 1) {
        printf("%d%s", 1, "Error server send");
        return 1;
    }
    if(pp_wait_completions(ctx, 1)) {
        printf("%s", "Error completions");
        return 1;
    }
    return 0;
}

/*Connect to server*/
int kv_open(char *servername, void **kv_handle) {
    return connect_main(servername,
                        argc_global,
                        argv_global,
                        (struct pingpong_context **)kv_handle);
}

int eager_kv_set(void *kv_handle, const char *key, const char *value) {
    struct pingpong_context *ctx = (struct pingpong_context*) kv_handle;
    ctx->size = sizeof (struct packet);
    struct packet *pack = (struct packet*)ctx->buf[ctx->currBuffer];
    pack->protocol_type = EAGER;
    pack->request_type = SET;
    strncpy(pack->key, key, sizeof(pack->key));
    strncpy(pack->value, value, sizeof(pack->value));
    if (send_packet(ctx)) {
        return 1;
    }
    return 0;
}

int rendezvous_kv_set (void *kv_handle, const char *key, const char *value) {
    struct pingpong_context *ctx = (struct pingpong_context*) kv_handle;
    struct packet *pack = (struct packet*)ctx->buf[ctx->currBuffer];
    pack->protocol_type = RENDEZVOUS;
    pack->request_type = SET;
    size_t size_value = strlen(value) + 1;
    pack->rendezvous_set.size = size_value;
    strcpy(pack->key,key);
    if (send_packet(ctx)) {
        return 1;
    }
    if(receive_packet(ctx)) {
        return 1;
    }
    struct packet *pack_response = (struct packet*)ctx->buf[ctx->currBuffer];

    struct ibv_mr* ctxMR = (struct ibv_mr*)ctx->mr[ctx->currBuffer];
    struct ibv_mr* clientMR = ibv_reg_mr(ctx->pd, (char *) value,
                                         size_value, IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);

    ctx->mr[ctx->currBuffer] = clientMR;
    ctx->size = size_value;
    pp_post_send(ctx,
                 value,
                 pack_response->rendezvous_set.remote_addr,
                 pack_response->rendezvous_set.rkey,
                 IBV_WR_RDMA_WRITE);
    if(pp_wait_completions(ctx, 1)) {
        printf("%s", "Error completions");
        return 1;
    };
    ctx->mr[ctx->currBuffer] = (struct ibv_mr*) ctxMR;
    // ---- FIN (ACK TO SERVER) -----
    ctx->size = 1;
    if (pp_post_send(ctx, NULL, NULL, 0, IBV_WR_SEND)) {
        printf("%d%s", 1, "Error server send");
        return 1;
    }
    if(pp_wait_completions(ctx, 1)) {
        printf("%s", "Error completions");
        return 1;
    }
    // --------------------------
    ibv_dereg_mr(clientMR);
    return 0;
}

int kv_set(void *kv_handle, const char *key, const char *value) {
    int response;
    struct pingpong_context *ctx = (struct pingpong_context*) kv_handle;
    unsigned packet_size = strlen(key) + strlen(value);
    if (packet_size < BITS_4_KB) {
        // EAGER PROTOCOL
        response = eager_kv_set(kv_handle, key, value);
    }
    else {
        // RENDEZVOUS PROTOCOL
        response = rendezvous_kv_set(kv_handle, key, value);
    }
    ctx->currBuffer = (ctx->currBuffer + 1) % MAXIMUM_HANDLE;
    return response;
}

int eager_kv_get(char **value, struct packet * response_pack) {
    *value = malloc(strlen(response_pack->response_value) + 1);
    strncpy(*value, response_pack->response_value, strlen(response_pack->response_value) + 1);
    return 0;
}

int rendezvous_kv_get (void *kv_handle, const char *key, char **value) {
    struct pingpong_context *ctx = (struct pingpong_context*) kv_handle;
    struct packet *pack = (struct packet*)ctx->buf[ctx->currBuffer];
    *value = malloc( pack->rendezvous_get.size);
    struct ibv_mr *temp_mr = ctx->mr[ctx->currBuffer];
    struct ibv_mr *curr_client_mr = ibv_reg_mr(ctx->pd,
                                               *value,
                                               pack->rendezvous_get.size,
                                               IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
    ctx->mr[ctx->currBuffer] = curr_client_mr;
    ctx->size = pack->rendezvous_get.size;
    if (pp_post_send(ctx,
                     *value,
                     pack->rendezvous_get.remote_addr,
                     pack->rendezvous_get.rkey,
                     IBV_WR_RDMA_READ)) {
        printf("%d%s", 1, "Error server send");
        return 1;
    }
    if(pp_wait_completions(ctx, 1)) {
        printf("%s", "Error completions");
        return 1;
    }
    ctx->mr[ctx->currBuffer] = temp_mr;
    ibv_dereg_mr(curr_client_mr);
    return 0;
}


int kv_get(void *kv_handle, const char *key, char **value) {
    struct pingpong_context *ctx = (struct pingpong_context*) kv_handle;
    struct packet *pack = (struct packet*)ctx->buf[ctx->currBuffer];
    pack->request_type = GET;
    strncpy(pack->key, key, sizeof(pack->key));
    if (send_packet(ctx)) {
        return 1;
    }
    if(receive_packet(ctx)) {
        return 1;
    }
    struct packet *response_pack = (struct packet*)ctx->buf[ctx->currBuffer];
    int response;
    if (response_pack->protocol_type == EAGER) {
        // EAGER PROTOCOL
        response = eager_kv_get(value, response_pack);
    }
    else {
        // RENDEZVOUS PROTOCOL
        response = rendezvous_kv_get(kv_handle, key, value);
    }
    ctx->currBuffer = (ctx->currBuffer + 1) % MAXIMUM_HANDLE;
    return response;
}

/* Called after get() on value pointer */
void kv_release(char *value) {
    free(value);
}

/* Destroys the QP */
int kv_close(void *kv_handle) {
    return pp_close_ctx(kv_handle);
}

void handle_server_set_request_rendezvous(struct pingpong_context * ctx,
                                          struct packet * pack, struct packet * packet_db, size_t buf_id) {
    struct packet * pack_response = (struct packet*) ctx->buf[buf_id];
//    free(packet_db->bigValue);
    packet_db->bigValue = calloc(pack->rendezvous_set.size, 1);
    packet_db->protocol_type = RENDEZVOUS;
    struct ibv_mr* mr_create = ibv_reg_mr(ctx->pd, packet_db->bigValue, pack->rendezvous_set.size,
                                          IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
    pack_response->request_type = SET;
    pack_response->protocol_type = RENDEZVOUS;
    pack_response->rendezvous_set.rkey = mr_create->rkey;
    pack_response->rendezvous_set.remote_addr = mr_create->addr;
    ctx->currBuffer = buf_id;
    send_packet(ctx);
    // WAIT FOR FIN
    ctx->size = 1;
    if (pp_post_recv(ctx, 1) != 1) {
        printf("%d%s", 1, "Error server send");
        return;
    }
    if(pp_wait_completions(ctx, 1)) {
        printf("%s", "Error completions");
        return;
    }
    // WAIT FOR FIN
}

int server_handle_set_request(struct pingpong_context *ctx, struct packet *pack, struct Database *database,
                              size_t buf_id) {
    if (database->curr_size == database->capacity) {
        struct packet ** new_map = malloc(sizeof(struct packet *) * DATABASE_FACTOR_CAPACITY * database->capacity);
        for (int i = 0; i < database->capacity * DATABASE_FACTOR_CAPACITY; i++) {
            if (i < database->curr_size) {
                new_map[i] = database->map[i];
            }
            else {
                new_map[i] = malloc(sizeof (struct packet));
            }

        }
        free(database->map);
        database->map = new_map;
        database->capacity *= DATABASE_FACTOR_CAPACITY;
    }
    for (int i = 0; i < database->curr_size; i++) {
        if(strcmp(database->map[i]->key, pack->key) == 0) {
            if (pack->protocol_type == EAGER) {
                // EAGER PROTOCOL
                strncpy(database->map[i]->value, pack->value, sizeof(pack->value));
                return 0;
            }
            // RENDEZVOUS PROTOCOL
            handle_server_set_request_rendezvous(ctx, pack, database->map[i], buf_id);
            return 0;
        }
    }
    // The key is not in our database
    strncpy(database->map[database->curr_size]->key, pack->key, sizeof(pack->key));
    if (pack->protocol_type == EAGER) {
        // EAGER PROTOCOL
        strncpy(database->map[database->curr_size]->value, pack->value, sizeof(pack->value));
        database->curr_size++;
        return 0;
    }
    // RENDEZVOUS PROTOCOL
    handle_server_set_request_rendezvous(ctx, pack, database->map[database->curr_size], buf_id);
    database->curr_size++;
    return 0;
}

int server_handle_get_request(struct pingpong_context *ctx, struct packet *pack, struct Database *database,
                              size_t buf_id) {
    struct packet *pack_response = (struct packet*)ctx->buf[buf_id];
    for (int i = 0; i < database->curr_size; i++) {
        if(strcmp(database->map[i]->key, pack->key) == 0){
            if (database->map[i]->protocol_type == RENDEZVOUS) {
                // RENDEZVOUS PROTOCOL
                size_t value_size = strlen(database->map[i]->bigValue) + 1;
                pack_response->protocol_type = RENDEZVOUS;
                struct ibv_mr* mr_create = ibv_reg_mr(ctx->pd, database->map[i]->bigValue, value_size,
                                                      IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
                pack_response->rendezvous_get.rkey = mr_create->rkey;
                pack_response->rendezvous_get.remote_addr = mr_create->addr;
                pack_response->rendezvous_get.size = value_size;
                ctx->currBuffer = buf_id;
                return send_packet(ctx);
            }
            // EAGER PROTOCOL
            pack_response->protocol_type = EAGER;
            strncpy(pack_response->response_value, database->map[i]->value, sizeof(pack_response->value));
            ctx->currBuffer = buf_id;
            return send_packet(ctx);
        }
    }
    pack_response->protocol_type = EAGER;
    strncpy(pack_response->response_value, "", sizeof(pack_response->response_value));
    ctx->currBuffer = buf_id;
    return send_packet(ctx);
}

int receive_packet_async(struct pingpong_context *ctx) {
    ctx->size = sizeof (struct packet);
    if (pp_post_recv(ctx, 1) != 1) {
        printf("%d%s", 1, "Error server receive");
        return 1;
    }
    return 0;
}

int handle_request(struct pingpong_context *ctx, struct Database *database, struct packet *pack, size_t buf_id) {
    switch (pack->request_type) {
        case SET:
            if (pack->protocol_type == EAGER) {
                printf("New Set Request:\n\tKEY: %s\n\tVALUE: %s\n", pack->key, pack->value);
            }
            else {
                printf("New Set Request:\n\tKEY: %s\n", pack->key);
            }
            fflush( stdout);
            return server_handle_set_request(ctx, pack, database, buf_id);
        case GET:
            printf("New Get Request:\n\tKEY: %s\n", pack->key);
            fflush( stdout);
            return server_handle_get_request(ctx, pack, database, buf_id);
    }
}

int handle_server(struct pingpong_context *ctx[NUMBER_OF_CLIENTS], struct Database *database, int number_of_clients) {
    size_t free_buff_ind[NUMBER_OF_CLIENTS] = {0};
    for (int curr_client = 0; curr_client < number_of_clients; curr_client++) {
        if(receive_packet_async(ctx[curr_client])) {
            return 1;
        }
    }
    while(1) {
        for (int curr_client = 0; curr_client < number_of_clients; curr_client++) {
            struct ibv_wc wc[WC_BATCH];
            int ne = ibv_poll_cq(ctx[curr_client]->cq, WC_BATCH, wc);
            if (ne < 0) {
                fprintf(stderr, "poll CQ failed %d\n", ne);
                return 1;
            }
            if (ne >= 1) {
                fflush(stdout);
                handle_request(ctx[curr_client], database,
                               (struct packet *) ctx[curr_client]->buf[free_buff_ind[curr_client]],
                               free_buff_ind[curr_client]);
                free_buff_ind[curr_client] = (free_buff_ind[curr_client] + 1) % MAXIMUM_HANDLE;
                ctx[curr_client]->currBuffer = free_buff_ind[curr_client];
                receive_packet_async(ctx[curr_client]);
            }
        }
    }
}

void run_tests(void *kv_handle) {
    // wait for two clients to connect to the server
    sleep(5);
    // EAGER PROTOCOL TEST
    char *value;
    char * KEY_1 = "First_key";
    char * VALUE_1 = "First value";
    char * KEY_2 = "Second_key";
    char * VALUE_2 = "Second value";
    char * SOME_KEY = "Some key";
    char * NULL_VALUE = "";
    kv_set(kv_handle, KEY_1, VALUE_1);
    kv_get(kv_handle, KEY_1, &value);
    assert(strcmp(VALUE_1, value) == 0);
    kv_set(kv_handle, KEY_2, VALUE_2);
    kv_get(kv_handle, KEY_1, &value);
    assert(strcmp(VALUE_1, value) == 0);
    kv_get(kv_handle, KEY_2, &value);
    assert(strcmp(VALUE_2, value) == 0);
    kv_get(kv_handle, SOME_KEY, &value);
    printf("%s\n", value);
    assert(strcmp(NULL_VALUE, value) == 0);
    kv_get(kv_handle, SOME_KEY, &value);
    assert(strcmp(NULL_VALUE, value) == 0);
    // RENDEZVOUS PROTOCOL TEST
    char * long_key = "rendezvous_key";
    char * long_value = malloc(32000 * 2);
    for (int i = 0; i < 32000 * 2; i++) {
        long_value[i] = 'a';
    }
    kv_set(kv_handle, long_key, long_value);
    kv_get(kv_handle, long_key, &value);
    assert(strcmp(long_value, value) == 0);
    free(long_value);
    printf("%s", "ALL TESTS PASSED!\n");
    fflush(stdout);
    kv_release(value);
    kv_close(kv_handle);
}

int run_server() {
    struct Database *database;
    void *kv_handle[NUMBER_OF_CLIENTS];
    database = malloc(sizeof (struct Database));
    database->map = malloc(sizeof(struct packet *) * DATABASE_INITIAL_CAPACITY);
    for (int i = 0; i < DATABASE_INITIAL_CAPACITY; i++) {
        database->map[i] = malloc(sizeof (struct packet));
    }
    database->capacity = DATABASE_INITIAL_CAPACITY;
    // Connect all clients to server
    for (int i = 0; i < NUMBER_OF_CLIENTS; i++) {
        kv_open(NULL, &kv_handle[i]);
    }
    // Handle clients requests
    handle_server((struct pingpong_context **) kv_handle, database, NUMBER_OF_CLIENTS);
    free(database->map);
    free(database);
    return 0;
}

int get_servername(char ** servername, int argc, char **argv) {
    argc_global = argc;
    argv_global = argv;
    if (optind == argc - 1)
        *servername = strdup(argv[optind]);
    else if (optind < argc) {
        usage(argv[0]);
        return 1;
    }
    return 0;
}

void test_performance(void *kv_handle) {
    long double real_iters = 100;
    long double warmp_up_iters = 50;
    char key[12];
    printf("\n%s\n\n", "+++++++++++++++ SET THROUGHPUT MEASURE +++++++++++++++");
    printf("%s\n", "------- Eager Protocol -------");
    for (long int message_size = 1;  message_size <= MAXIMUM_SIZE; message_size *= 2) {
        if (message_size == BITS_4_KB) {
            printf("%s\n", "------- Rendezvous Protocol -------");
        }
        snprintf(key, sizeof (key), "%ld", message_size);
        char * largeValue = calloc(message_size, sizeof(char));
        memset(largeValue, 'a', message_size - 1);
        clock_t start_time;
        for (int i = 0; i < real_iters + warmp_up_iters; i++) {
            if (i == warmp_up_iters) {
                start_time = clock();
            }
            kv_set(kv_handle, key, largeValue);
        }
        clock_t end_time = clock();
        long double diff_time = (long double) (end_time - start_time) / CLOCKS_PER_SEC;
        long double gb_unit = real_iters * message_size / pow(1024, 3);
        long double throughput = gb_unit / diff_time;
        printf("%ld\t%Lf\t%s\n", message_size, throughput, "Gigabytes/Second");
    }
    printf("\n%s\n\n", "+++++++++++++++ GET THROUGHPUT MEASURE +++++++++++++++");
    printf("%s\n", "------- Eager Protocol -------");
    for (long int message_size = 1;  message_size <= MAXIMUM_SIZE; message_size *= 2) {
        if (message_size == BITS_4_KB) {
            printf("%s\n", "------- Rendezvous Protocol -------");
        }
        snprintf(key, sizeof (key), "%ld", message_size);
        char * largeValue = calloc(message_size, sizeof(char));
        memset(largeValue, 'a', message_size - 1);
        char *retrievedValue = NULL;
        clock_t start_time;
        for (int i = 0; i < real_iters + warmp_up_iters; i++) {
            if (i == warmp_up_iters) {
                start_time = clock();
            };
            kv_get(kv_handle, key, &retrievedValue);
            if (!compareStrings(retrievedValue, largeValue)) {
                printf(RED "THROUGHPUT: GET request for large value failed.\n" RESET);
                exit(1);
            }
        }
        free(largeValue);
        clock_t end_time = clock();
        long double diff_time = (long double) (end_time - start_time) / CLOCKS_PER_SEC;
        long double gb_unit = real_iters * message_size / pow(1024, 3);
        long double throughput = gb_unit / diff_time;
        printf("%ld\t%Lf\t%s\n", message_size, throughput, "Gigabytes/Second");
    }
}

int run_client (char * servername) {
    // CODE TEST - ONE CLIENT
    run_tests_one_client(servername);
    // CODE TEST - MULTIPLE CLIENTS
//    run_tests_multiple_clients(servername);
    // THROUGHPUT TEST
//    void *kv_handle;
//    kv_open(servername, &kv_handle);
//    test_performance(kv_handle);
    return 0;
}

int main(int argc, char **argv)
{
    char *servername;
    get_servername(&servername, argc, argv);
    return servername ? run_client(servername) : run_server();
}
