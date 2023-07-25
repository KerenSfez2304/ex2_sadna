//#include <stdio.h>
//#include <string.h>
//#include "map.h"
//
//int main() {
//  // Create a map
//  hashmap* map = hashmap_create();
//
//  // Add a key-value pair
//  const char* key = "example";
//  const char *str = "adfghdfgha";
////  uintptr_t value[2] = {42, 43};
//  hashmap_set(map, key, strlen(key), (uintptr_t) str);
//
//  // Get the value based on the key
//  uintptr_t retrievedValue;
//  if (hashmap_get(map, key, strlen(key), &retrievedValue)) {
////      printf("Retrieved value: %hhd, %hhd, %hhd\n", ((char *)retrievedValue)[0], ((char *)retrievedValue)[1], ((char *)retrievedValue)[2]);
//      printf("Retrieved value: %s\n", (char *)retrievedValue);
//    } else {
//      printf("Key not found\n");
//    }
//
//  // Get a non-existing value
//  uintptr_t retrievedValue2;
//  if (hashmap_get(map, "nonexisting", strlen("nonexisting"), &retrievedValue2)) {
//      printf("Retrieved value: %lu\n", retrievedValue);
//    } else {
//      printf("Key not found\n");
//    }
//
//  // Free the map
//  hashmap_free(map);
//
//  return 0;
//}


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
#include "map.h"

#include <infiniband/verbs.h>

#define WC_BATCH (10)

#define WARM_UP_AMOUNT 500
#define DATA_AMOUNT 1000
#define MEGABYTE 1048576
#define SET_PREFIX_SIZE 2
#define GET_PREFIX_SIZE 1
#define SET_KEY_SIZE 2
#define INIT_MSG_SIZE 8
#define MAX_EAGER_MSG_SIZE 4096
#define SET_BUFFER_SIZE 4096

/* the initial kv_store context will have 4 buffers of size 4K.
 * buffers 0-4: receive buffers.
 * buffers 5-9: send buffers.*/
#define BUFS_AMOUNT 10
#define RECV_BUFS_AMOUNT 5
#define MAX_RECV_BUF_INDEX 4
#define REND_WR_ID 11
#define SINGLE_CLIENT 1
#define TWO_CLIENTS 2
enum {
    PINGPONG_RECV_WRID = 1,
    PINGPONG_SEND_WRID = 2,
};

static int page_size;


struct handle
{
    hashmap *map;
    struct ibv_device **dev_list;
    struct pingpong_context *ctx;
    struct pingpong_dest *rem_dest;
    int last_wr_id;
};


struct pingpong_context {
    struct ibv_context		*context;
    struct ibv_comp_channel	*channel;
    struct ibv_pd		*pd;
    struct ibv_mr		*mr;
    struct ibv_cq		*cq;
    struct ibv_qp		*qp;
    void			*buf;
    int             free_set_bufs_amount;
    // 0 for buffer not free to use, 1 for buffer free to use
    int             free_bufs[BUFS_AMOUNT];
    // 0 for buffer not pending request, 1 for buffer pending request
    int             pending_request_bufs[RECV_BUFS_AMOUNT];
    int				size;
    int				rx_depth;
    int				routs;
    struct ibv_port_attr	portinfo;
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

int kv_get_port_info(struct ibv_context *context, int port,
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

static int kv_connect_ctx(struct pingpong_context *ctx, int port, int my_psn,
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

static struct pingpong_dest *kv_client_exch_dest(const char *servername, int port,
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

static struct pingpong_dest *kv_server_exch_dest(struct pingpong_context *ctx,
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

  if (kv_connect_ctx (ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx)) {
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

static struct pingpong_context *kv_init_ctx(struct ibv_device *ib_dev, int size,
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

  ctx->buf = malloc(roundup(size, page_size));
  if (!ctx->buf) {
      fprintf(stderr, "Couldn't allocate work buf.\n");
      return NULL;
    }

  // initialize the ctx with 10 free buffers
  ctx->free_set_bufs_amount = BUFS_AMOUNT;
  for (int i = 0; i < BUFS_AMOUNT; i++){
      ctx->free_bufs[i] = 1;
    }

  memset(ctx->buf, 0x7b + is_server, size);

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

  ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, size, IBV_ACCESS_LOCAL_WRITE);
  if (!ctx->mr) {
      fprintf(stderr, "Couldn't register MR\n");
      return NULL;
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

  if (ibv_dereg_mr(ctx->mr)) {
      fprintf(stderr, "Couldn't deregister MR\n");
      return 1;
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
  free(ctx);

  return 0;
}

static int kv_post_recv(struct handle *handle, int free_buf)
{
  int index = free_buf * SET_BUFFER_SIZE;

  struct ibv_sge list = {
      .addr	= (uintptr_t) &(handle->ctx->buf[index]),
      .length = SET_BUFFER_SIZE,
      .lkey	= handle->ctx->mr->lkey
  };

  struct ibv_recv_wr wr = {
      .wr_id	    = free_buf,
      .sg_list    = &list,
      .num_sge    = 1,
      .next       = NULL
  };
  struct ibv_recv_wr *bad_wr;

  if (ibv_post_recv(handle->ctx->qp, &wr, &bad_wr)){
      fprintf(stderr, "Failed to post receive.");
      return 1;
    }

  return 0;
}


static int kv_post_send_rdma_read(struct handle *handle, struct ibv_mr	*mr,
                                  void *rdma_buf, size_t *msg_size,
                                  void **remote_addr, uint32_t *rkey){
  struct ibv_sge list = {
      .addr	= (uintptr_t) rdma_buf,
      .length = *msg_size,
      .lkey	= mr->lkey
  };
  struct ibv_send_wr *bad_wr, wr = {
      .wr_id	    = REND_WR_ID,
      .sg_list    = &list,
      .num_sge    = 1,
      .opcode     = IBV_WR_RDMA_READ,
      .send_flags = IBV_SEND_SIGNALED,
      .wr.rdma.remote_addr = *remote_addr,
      .wr.rdma.rkey        = *rkey
  };

  return ibv_post_send(handle->ctx->qp, &wr, &bad_wr);
}

static int kv_post_send(struct handle *handle, int send_buf, int send_buf_index){
  handle->ctx->free_set_bufs_amount--;
  handle->ctx->free_bufs[send_buf] = 0;

  struct ibv_sge list = {
      .addr	= (uint64_t)(&(handle->ctx->buf[send_buf_index])),
      .length = SET_BUFFER_SIZE,
      .lkey	= handle->ctx->mr->lkey
  };

  uint32_t inline_flag = 0;
//  if (handle->ctx->size <= 32){
//      inline_flag = IBV_SEND_INLINE;
//    }

  struct ibv_send_wr *bad_wr, wr = {
      .wr_id	    = send_buf,
      .sg_list    = &list,
      .num_sge    = 1,
      .opcode     = IBV_WR_SEND,
      .send_flags = IBV_SEND_SIGNALED | inline_flag,
      .next       = NULL
  };

  return ibv_post_send(handle->ctx->qp, &wr, &bad_wr);
}

int kv_wait_completion_recv(struct handle *handle, struct ibv_wc *wc){
  int completed_amount, i;

  // while no work has been completed, keep on polling
  do {
      completed_amount = ibv_poll_cq(handle->ctx->cq, 1, wc);
      if (completed_amount < 0) {
          fprintf(stderr, "poll CQ failed %d\n", completed_amount);
          return 1;
        }

    } while (completed_amount < 1);

  if (wc->status != IBV_WC_SUCCESS) {
      fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
              ibv_wc_status_str(wc->status),
              wc->status, (int) wc->wr_id);
      return 1;
    }

  if ((int) wc->wr_id > MAX_RECV_BUF_INDEX && (int) wc->wr_id < BUFS_AMOUNT){ // send buffer
      handle->ctx->free_set_bufs_amount++;
      handle->ctx->free_bufs[(int) wc->wr_id] = 1;
    }
//  else if ((int) wc->wr_id <= MAX_RECV_BUF_INDEX) { //recv buffer
//      handle->ctx->pending_request_bufs[(int) wc->wr_id] = 1;
//    } //TODO delete:  kv_wait_completion_recv is called always when waiting for a single recv item, so no need to mark as pending
  handle->last_wr_id = wc->wr_id;

  return 0;
}

/* Poll amount_to_poll CQ items from the completion queue. amount_to_poll has
 * to be <= 10.*/
int kv_wait_completions(struct handle *handle, int amount_to_poll){
  struct ibv_wc wc[WC_BATCH];
  int completed_amount, i;
  // while no work has been completed, keep on polling
  do {
      completed_amount = ibv_poll_cq(handle->ctx->cq, amount_to_poll, wc);
      if (completed_amount < 0) {
          fprintf(stderr, "poll CQ failed %d\n", completed_amount);
          return 1;
        }

    } while (completed_amount < 1);


  for (i = 0; i < completed_amount; ++i) {
      if (wc[i].status != IBV_WC_SUCCESS) {
          fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                  ibv_wc_status_str(wc[i].status),
                  wc[i].status, (int) wc[i].wr_id);
          return 1;
        }
      if ((int) wc[i].wr_id > MAX_RECV_BUF_INDEX && (int) wc[i].wr_id < BUFS_AMOUNT){ // send buffer
          handle->ctx->free_set_bufs_amount++;
          handle->ctx->free_bufs[(int) wc[i].wr_id] = 1;
        }
      else if ((int) wc[i].wr_id <= MAX_RECV_BUF_INDEX) { //recv buffer
          handle->ctx->pending_request_bufs[(int) wc[i].wr_id] = 1;
        }
      handle->last_wr_id = wc[i].wr_id;
    }
  return 0;
}


static void usage()
{
  printf("Usage:\n");
  printf("              start a server and wait for connection\n");
  printf("   <host>     connect to server at <host>\n");
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

void print_throughput(double time_taken, size_t msg_size){
  long double cur_throughput = (DATA_AMOUNT * (long double) msg_size) / time_taken;
  fprintf(stdout, "%zu\t%Lf\tBytes/microsecond\n", msg_size, cur_throughput);
}


int wait_for_buf(struct handle *handle){

  // if there is no free set set_buffer- wait for one
  if (handle->ctx->free_set_bufs_amount == 0){
      kv_wait_completions (handle, WC_BATCH);
    }

  // find a free buffer
  int free_buf;
  for (int i = 0; i < BUFS_AMOUNT; i++){
      if (handle->ctx->free_bufs[i] == 1){
          free_buf = i;
          break;
        }
    }
  return free_buf;
}

int kv_eager_set(struct handle *handle, const char *key, const char *value,
                 size_t keylen, size_t vallen){

  // insert a message to the set set_buffer, with 2 bytes of info (message type e/r,
  // request type s/g) and then the key and value (including null terminators)
  int free_buf = wait_for_buf(handle);
  int free_buf_index = free_buf * SET_BUFFER_SIZE;
  char *set_buffer = (char *)(&(handle->ctx->buf[free_buf_index]));
  set_buffer[0] = 'e';
  set_buffer[1] = 's';
  memcpy(set_buffer + SET_PREFIX_SIZE, key, keylen);
  memcpy(set_buffer + SET_PREFIX_SIZE + keylen, value, vallen);
  if (kv_post_send (handle, free_buf, free_buf_index)) {
      fprintf(stderr, "Client couldn't post send.\n");
      return 1;
    }
  return 0;
}

int allocate_server_rdma_buf(struct handle *handle, const char *value,
                             size_t vallen, struct ibv_mr **mr){
  *mr= ibv_reg_mr(handle->ctx->pd, value, vallen,
                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
  if (!(*mr)) {
      fprintf(stderr, "Couldn't register MR\n");
      return 1;
    }

  return 0;

}

int allocate_rdma_buf(struct handle *handle,
                      const char *value, size_t vallen, struct ibv_mr **mr){
//  size_t rdma_buf_size = keylen + vallen;
//  *rdma_buf = (char *) malloc(sizeof (char) * (rdma_buf_size));

//  if (!(*rdma_buf)) {
//      fprintf(stderr, "Couldn't allocate value.\n");
//      return 1;
//    }

//  memcpy(*rdma_buf, key, keylen);
//  memcpy(*rdma_buf + keylen, value, vallen);

  *mr= ibv_reg_mr(handle->ctx->pd, value, vallen,
                  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
  if (!(*mr)) {
      fprintf(stderr, "Couldn't register MR\n");
      return 1;
    }

  return 0;
}

int kv_rend_set(struct handle *handle, const char *key, const char *value,
                size_t keylen, size_t vallen){
  // allocate RDMA buffer
  struct ibv_mr	*mr;
  allocate_rdma_buf (handle, value, vallen, &mr);

  // send ctrl message
  int free_buf = wait_for_buf(handle);
  int free_buf_index = free_buf * SET_BUFFER_SIZE;
  char *control_buf = (char *)(&(handle->ctx->buf[free_buf_index]));

  // the message format is rs{addr}{rkey}{value_len}{key}
  control_buf[0] = 'r';
  control_buf[1] = 's';
  size_t addrlen = sizeof(void *);
  size_t rkeylen = sizeof(uint32_t);
  size_t vallen_size = sizeof(size_t);

  memcpy(control_buf + SET_PREFIX_SIZE, &(mr->addr), addrlen);
  memcpy(control_buf + SET_PREFIX_SIZE + addrlen, &(mr->rkey), rkeylen);
  memcpy(control_buf + SET_PREFIX_SIZE + addrlen + rkeylen, &vallen, vallen_size);
  memcpy(control_buf + SET_PREFIX_SIZE + addrlen + rkeylen + vallen_size, key, keylen);

//  size_t written_msg_size;
//  memcpy(&written_msg_size, control_buf + SET_PREFIX_SIZE + addrlen + rkeylen, sizeof(size_t));

  if (kv_post_send (handle, free_buf, free_buf_index)) {
      fprintf(stderr, "Client couldn't post send.\n");
      return 1;
    }

  // wait for completions of work items, until the completion of this rend set request
  do {
      kv_wait_completions(handle, 1);
    }
  while(handle->last_wr_id != free_buf);

  // get fin - wait until the last completed item belongs to a post_recv
  struct ibv_wc wc;
  do {
      if (kv_wait_completion_recv (handle, &wc) == 1) {
          fprintf (stderr, "server couldn't poll for a completion queue item.");
          return 1;
        }
    }
  while(handle->last_wr_id > MAX_RECV_BUF_INDEX);

  // verify that the received message was a FIN
  int fin_buf_index = handle->last_wr_id * SET_BUFFER_SIZE;
  char *fin_buf = (char *) &(handle->ctx->buf[fin_buf_index]);
  if (!(fin_buf[0] == 'f' && fin_buf[1] == 'i' && fin_buf[2] == 'n')){
      fprintf(stderr, "Received invalid message while waiting for rend set FIN from server."
                      "messages received: %s", fin_buf);
      return 1;
    }

  // reuse recv_buf
  if (kv_post_recv(handle, handle->last_wr_id)){
      fprintf(stderr, "failed to post_recv inside kv_rend_set.\n");
      return 1;
    }

  // free mr
  if (ibv_dereg_mr(mr)) {
      fprintf(stderr, "Couldn't deregister MR inside kv_rend_set.\n");
      return 1;
    }

  return 0;
}

int kv_set(void *kv_handle, const char *key, const char *value){
  struct handle *handle = (struct handle*)kv_handle;

  size_t keylen = strlen(key) + 1;
  size_t vallen = strlen(value) + 1;
  size_t msg_size = SET_PREFIX_SIZE + keylen + vallen;
  if (msg_size > MAX_EAGER_MSG_SIZE){
      if (kv_rend_set(handle, key, value, keylen, vallen)){
          fprintf(stderr, "Client couldn't send rend set request. key: %s, value: %s\n", key, value);
          return 1;
        }
    }

  else {
      if (kv_eager_set(handle, key, value, keylen, vallen)){
          fprintf(stderr, "Client couldn't send eager set request. key: %s, value: %s\n", key, value);
          return 1;
        }
    }

  return 0;
}

int handle_eager_get_from_server(struct handle *handle, char *buffer, char **value, int value_buf){
  fprintf(stdout, "got here 7\n");
  fflush(stdout);
  // copy response
  size_t vallen = strlen(&buffer[1]) + 1;
  *value = (char *) malloc(sizeof(char) * (vallen));
  if (!(*value)) {
      fprintf(stderr, "Couldn't allocate value.\n");
      return 1;
    }
  memcpy(*value, &buffer[1], vallen);
  fprintf(stdout, "got here 8\n");
  fflush(stdout);
  // reuse value buffer
  kv_post_recv(handle, value_buf);
  fprintf(stdout, "got here 9\n");
  fflush(stdout);
}

int handle_rend_get_from_server(struct handle *handle, char *buffer, char **value, int value_buf){
  // extract server's addr, rkey and the required value size
  // rend get format is: r{addr}{rkey}{value_size}
  void **addr;
  uint32_t *rkey;
  size_t *value_len;

  // the size of the addr in the message is the size of a pointer
  size_t addrlen = sizeof(int *);
  size_t rkeylen = sizeof(uint32_t);

  addr = (void **) (buffer + GET_PREFIX_SIZE);
  rkey = (uint32_t *) (buffer + GET_PREFIX_SIZE + addrlen);
  value_len = (size_t *) (buffer + GET_PREFIX_SIZE + addrlen + rkeylen);

  // allocate buffer for value and register it as mr
  char *rdma_buf = (char *) malloc(sizeof(char) * (*value_len));
  if (!rdma_buf){
      fprintf(stderr, "client failed to allocate rdma buffer.\n");
      return 1;
    }

  struct ibv_mr	*mr;
  mr = ibv_reg_mr(handle->ctx->pd, rdma_buf, *value_len, IBV_ACCESS_LOCAL_WRITE);
  if (!(mr)) {
      fprintf(stderr, "Couldn't register MR\n");
      return 1;
    }

  // post send a work with RDMA_READ opcode to read value from server
  if (kv_post_send_rdma_read (handle, mr, rdma_buf, value_len, addr, rkey)) {
      fprintf(stderr, "Server couldn't post send an RDMA_READ item.\n");
      return 1;
    }

  // wait for completion of this read operation
  do {
      kv_wait_completions(handle, 1);
    }
  while(handle->last_wr_id != REND_WR_ID);

  // extract value
  size_t vallen = strlen(rdma_buf) + 1;
  *value = (char *) malloc(sizeof (char) * vallen);
  if (!(*value)){
      fprintf(stderr, "failed to allocate value.\n");
      return 1;
    }

  strcpy(*value, rdma_buf);
  fprintf(stdout, "value is: %s\n", *value);
  fflush(stdout);

  // send FIN message
  int free_buf = wait_for_buf(handle);
  int free_buf_index = free_buf * SET_BUFFER_SIZE;

  char *fin_buffer = (char *)(&(handle->ctx->buf[free_buf_index]));
  fin_buffer[0] = 'f';
  fin_buffer[1] = 'i';
  fin_buffer[2] = 'n';

  if (kv_post_send (handle, free_buf, free_buf_index)) {
      fprintf(stderr, "Client couldn't post send FIN message.\n");
      return 1;
    }

  // free RDMA buf, dereg mr
  if (ibv_dereg_mr(mr)) {
      fprintf(stderr, "Couldn't deregister MR inside kv_rend_set.\n");
      return 1;
    }

  free(rdma_buf);

  return 0;
}

int kv_get(void *kv_handle, const char *key, char **value){
  struct handle *handle = (struct handle*) kv_handle;

  size_t keylen = strlen(key) + 1;

  // insert a message to the ctx buffer, with 1 bytes of info (message type g)
  // and then the key
  int free_buf = wait_for_buf(handle);
  int free_buf_index = free_buf * SET_BUFFER_SIZE;
  fprintf(stdout, "got here 2\n");
  fflush(stdout);
  char *get_buffer = (char *)(&(handle->ctx->buf[free_buf_index]));
  get_buffer[0] = 'g';
  memcpy(get_buffer + GET_PREFIX_SIZE, key, keylen);
  fprintf(stdout, "got here 3\n");
  fflush(stdout);
  if (kv_post_send(handle, free_buf, free_buf_index)) {
      fprintf(stderr, "Client couldn't post send.\n");
      return 1;
    }
  fprintf(stdout, "got here 4\n");
  fflush(stdout);

  // wait until completion of recv item with the value
  struct ibv_wc wc;
  do {
      if (kv_wait_completion_recv (handle, &wc)){
          fprintf(stderr, "Failed to poll server's response for get request.");
          return 1;
        }
    }
    // if wr_id > 4 it belongs to a send item
  while (handle->last_wr_id > MAX_RECV_BUF_INDEX);

  fprintf(stdout, "got here 5\n");
  fflush(stdout);
  int value_buf = handle->last_wr_id;

  int value_buf_index = value_buf * SET_BUFFER_SIZE;
  char *buffer = (char *) &((handle->ctx->buf)[value_buf_index]);
  fprintf(stdout, "got here 6\n");
  fflush(stdout);
  // check response type- eager or rend
  if (buffer[0] == 'e'){
      if(handle_eager_get_from_server(handle, buffer, value, value_buf)){
          fprintf(stderr, "Failed to handle an eager response from the server"
                          "to the client's get request.");
          return 1;
        }
    }

  else if (buffer[0] == 'r'){
      if(handle_rend_get_from_server(handle, buffer, value, value_buf)){
          fprintf(stderr, "Failed to handle a rend response from the server"
                          "to the client's get request.");
          return 1;
        }
    }

  else {
      fprintf(stderr, "Received invalid response from server to get request.");
      return 1;
    }

  return 0;
}


int send_set_requests(struct handle *kv_handle, const char *key, const char *value,
                      int iters){
  int i;
  for (i = 0; i < iters; i++) {
//      if ((i != 0) && (i % handle->tx_depth == 0)) {
//          pp_wait_completions(handle, handle->tx_depth);
//        }
      if (kv_set(kv_handle, key, value)) {
          fprintf(stderr, "Client couldn't post send\n");
          return 1;
        }
//        fprintf(stdout, "sent %d set requests\n", i);
//        fflush(stdout);
    }
//  // complete sending of last 100 messages
//  pp_wait_completions(handle, handle->tx_depth);

  return 0;
}

int send_get_requests (struct handle *handle, const char *key, int iters){
  int i;
  char *value;


  for (i = 0; i < iters; i++) {
      if (kv_get(handle, key, &value)) {
          fprintf(stderr, "Client couldn't post send\n");
          return 1;
        }
//      fprintf(stdout, "sent %d get requests\n", i);
//      fflush(stdout);
    }

  free(value);
  return 0;
}


int parse_eager_set_request(struct handle *handle, char *msg){
  size_t keylen = strlen(&(msg[SET_PREFIX_SIZE])) + 1;
  size_t vallen = strlen(&(msg[SET_PREFIX_SIZE + keylen])) + 1;
  char *key = (char *) malloc(sizeof(char) * (keylen));
  if (!key){
      fprintf(stderr, "Failed to allocate a value.");
      return 1;
    }
  strcpy(key, &msg[SET_PREFIX_SIZE]);
  fprintf (stdout, "server got here 4\n");
  fflush(stdout);
  char *value = (char *) malloc(sizeof(char) * (vallen));
  if (!value){
      fprintf(stderr, "Failed to allocate a value.");
      return 1;
    }
  strcpy(value, &msg[SET_PREFIX_SIZE + keylen]);
  uintptr_t cur_val;
  fprintf (stdout, "server got here 5\n");
  fflush(stdout);
  if (hashmap_get(handle->map, key, keylen, &cur_val)){
      fprintf (stdout, "inside hashmap get. key is: %s, value is: %s\n", key, (char *) cur_val);
      fflush(stdout);
      free((void *) cur_val);
    }
  fprintf (stdout, "server got here 6\n");
  fflush(stdout);
  hashmap_set(handle->map, key, keylen, (uintptr_t)value);
  if (hashmap_get(handle->map, key, keylen, &cur_val)){
    }
  return 0;
}

int server_handle_eager_get_request(struct handle *handle, uintptr_t value,
                                    size_t vallen){
  // find a free send buffer, write in it: e{value}\0.
  int free_buf = wait_for_buf(handle);
  int free_buf_index = free_buf * SET_BUFFER_SIZE;

  char *set_buffer = (char *)(&(handle->ctx->buf[free_buf_index]));
  set_buffer[0] = 'e';
  memcpy(set_buffer + 1, (char*)value, vallen);
  if (kv_post_send (handle, free_buf, free_buf_index)) {
      fprintf(stderr, "Client couldn't post send\n");
      return 1;
    }
  if (kv_wait_completions (handle, 1)){
      fprintf(stderr, "Failed to poll completion of a response containing the value.");
      return 1;
    }

  return 0;
}

int server_handle_rend_get_request(struct handle *handle, uintptr_t value,
                                   size_t vallen){
  // allocate buffer for value, create mr
  struct ibv_mr	*mr;
  allocate_server_rdma_buf (handle, (char *)value, vallen, &mr);

  // send control msg to client with addr, rkey, value_size
  int free_buf = wait_for_buf(handle);
  int free_buf_index = free_buf * SET_BUFFER_SIZE;
  char *control_buf = (char *)(&(handle->ctx->buf[free_buf_index]));

  // the message format is r{addr}{rkey}{valuesize}
  control_buf[0] = 'r';
  size_t addrlen = sizeof(void *);
  size_t rkeylen = sizeof(uint32_t);

  memcpy(control_buf + GET_PREFIX_SIZE, &(mr->addr), addrlen);
  memcpy(control_buf + GET_PREFIX_SIZE + addrlen, &(mr->rkey), rkeylen);
  memcpy(control_buf + GET_PREFIX_SIZE + addrlen + rkeylen, &vallen, sizeof(size_t));

//  size_t written_msg_size;
//  memcpy(&written_msg_size, control_buf + GET_PREFIX_SIZE + addrlen + rkeylen, sizeof(size_t));

  if (kv_post_send (handle, free_buf, free_buf_index)) {
      fprintf(stderr, "Client couldn't post send.\n");
      return 1;
    }

  // wait for post_send completion of control msg
  do {
      kv_wait_completions(handle, 1);
    }
  while(handle->last_wr_id != free_buf);

  // wait for FIN - wait until the last completed item belongs to a post_recv
  struct ibv_wc wc;
  do {
      if (kv_wait_completion_recv (handle, &wc) == 1) {
          fprintf (stderr, "server couldn't poll for a completion queue item.");
          return 1;
        }
    }
  while(handle->last_wr_id > MAX_RECV_BUF_INDEX);

  // verify that the received message was a FIN
  int fin_buf_index = handle->last_wr_id * SET_BUFFER_SIZE;
  char *fin_buf = (char *) &(handle->ctx->buf[fin_buf_index]);
  if (!(fin_buf[0] == 'f' && fin_buf[1] == 'i' && fin_buf[2] == 'n')){
      fprintf(stderr, "Received invalid message while waiting for rend get FIN from client."
                      "messages received: %s", fin_buf);
      return 1;
    }

  // reuse recv_buf
  if (kv_post_recv(handle, handle->last_wr_id)){
      fprintf(stderr, "failed to post_recv inside server_handle_rend_get_request.\n");
      return 1;
    }

  // free mr
  if (ibv_dereg_mr(mr)) {
      fprintf(stderr, "Couldn't deregister MR inside server_handle_rend_get_request.\n");
      return 1;
    }

  return 0;
}


int parse_get_request(struct handle *handle, char *msg){
  uintptr_t value;
  size_t keysize = strlen(&msg[1]) + 1;
  hashmap_get(handle->map, &msg[1], keysize, &value);
  size_t vallen = strlen((char *)value) + 1;
  // msg will consists of char (e/r) and value
  size_t msg_size = vallen + 1;

  //rend
  if (msg_size > MAX_EAGER_MSG_SIZE){
      if (server_handle_rend_get_request (handle, value, vallen)){
          fprintf(stderr, "Server failed to parse get request with size > 4K.");
          return 1;
        }
    }

    //eager
  else {
      if (server_handle_eager_get_request (handle, value, vallen)){
          fprintf(stderr, "Server failed to parse get request with size <= 4K.");
          return 1;
        }
    }
  return 0;
}

int parse_eager_request(struct handle *handle, char *msg){
  if (msg[1] == 's'){
      if (parse_eager_set_request(handle, msg)){
          fprintf(stderr, "Failed to parse an eager client request.\n");
          return 1;
        }
    }
  else{
      fprintf(stderr, "Failed to parse an eager client request, invalid usage."
                      "received msg is: %s", msg);
      return 1;
    }

  return 0;
}

int parse_rendezvous_request(struct handle *handle, const char *msg){
  // extract rend information from message. set rend message format is:
  // rs{addr}{rkey}{value_len}{key}
  void **addr;
  uint32_t *rkey;
  size_t *vallen;

  // the size of the addr in the message is the size of a pointer
  size_t addrlen = sizeof(int *);
  size_t rkeylen = sizeof(uint32_t);
  size_t vallen_size = sizeof(size_t);

  addr = (void **) (msg + SET_PREFIX_SIZE);
  rkey = (uint32_t *) (msg + SET_PREFIX_SIZE + addrlen);
  vallen = (size_t *) (msg + SET_PREFIX_SIZE + addrlen + rkeylen);

  size_t keylen = strlen(msg + SET_PREFIX_SIZE + addrlen + rkeylen + vallen_size) + 1;
  char *key = (char *) malloc(sizeof(char) * keylen);
  strcpy(key, msg + SET_PREFIX_SIZE + addrlen + rkeylen + vallen_size);

  // allocate buffer to read value into
  char *rdma_buf = (char *) malloc(sizeof(char) * (*vallen));
  if (!rdma_buf){
      fprintf(stderr, "server failed to allocate rdma buffer.\n");
      return 1;
    }

  // register read buffer as mr
  struct ibv_mr	*mr;
  mr = ibv_reg_mr(handle->ctx->pd, rdma_buf, *vallen, IBV_ACCESS_LOCAL_WRITE);
  if (!(mr)) {
      fprintf(stderr, "Couldn't register MR\n");
      return 1;
    }

  // post send a work with RDMA_READ opcode to read value from client
  if (kv_post_send_rdma_read (handle, mr, rdma_buf, vallen, addr, rkey)) {
      fprintf(stderr, "Server couldn't post send an RDMA_READ item.\n");
      return 1;
    }

  // wait for completion of this read operation
  do {
      kv_wait_completions(handle, 1);
    }
  while(handle->last_wr_id != REND_WR_ID);

  // set new key-value in map
  uintptr_t cur_val;
  if (hashmap_get(handle->map, key, keylen, &cur_val)){

      free((void *) cur_val);
    }
  hashmap_set(handle->map, key, keylen, (uintptr_t)rdma_buf);

  // send FIN message
  int free_buf = wait_for_buf(handle);
  int free_buf_index = free_buf * SET_BUFFER_SIZE;

  char *fin_buffer = (char *)(&(handle->ctx->buf[free_buf_index]));
  fin_buffer[0] = 'f';
  fin_buffer[1] = 'i';
  fin_buffer[2] = 'n';

  if (kv_post_send (handle, free_buf, free_buf_index)) {
      fprintf(stderr, "Server couldn't post send FIN message.\n");
      return 1;
    }

  // free RDMA buf, dereg mr
  if (ibv_dereg_mr(mr)) {
      fprintf(stderr, "Couldn't deregister MR inside kv_rend_set.\n");
      return 1;
    }
//  free(rdma_buf); //TODO: delete this. shouldn't free rdma_buf because this is the value currently in the dictionary (it will be freed when this value is replaced in the future)

  return 0;
}

int server_parse_request(struct handle *handle, int recv_buf){
//  // poll a single work item from the completion queue, which corresponds
//  // to a receive item. the received message is written to a specific
//  // sub-buffer indicated by the wr_id
//  struct ibv_wc wc;
//  do {
//    if (kv_wait_completion_recv (handle, &wc) == 1) {
//      fprintf (stderr, "server couldn't poll for a completion queue item.");
//      return 1;
//    }
//  }
//  while (wc.wr_id > MAX_RECV_BUF_INDEX);

  int buffer_index = (recv_buf) * SET_BUFFER_SIZE;
  char *msg = &(((char *)(handle->ctx->buf))[buffer_index]);
  fprintf (stdout, "server got here 2\n");
  fflush(stdout);
  if (msg[0] == 'e'){
      fprintf (stdout, "server got here 3\n");
      fflush(stdout);
      if (parse_eager_request(handle, msg)){
          fprintf(stderr, "Failed to parse an eager client request.\n");
          return 1;
        }
    }

  else if (msg[0] == 'r'){
      if (parse_rendezvous_request(handle, msg)){
          fprintf(stderr, "Failed to parse a rendezvous client request.\n");
          return 1;
        }
    }

  else if (msg[0] == 'g'){
      if (parse_get_request(handle, msg)){
          fprintf(stderr, "Failed to parse a client's get request.\n");
          return 1;
        }
    }

  else{
      fprintf(stderr, "Failed to parse a client request, invalid usage. received request: %s", msg);
      return 1;
    }
  fprintf (stdout, "server got here 4\n");
  fflush(stdout);
  if (kv_post_recv(handle, recv_buf)){
      fprintf(stderr, "Failed to post receive.");
      return 1;
    }
  fprintf (stdout, "server got here 5\n");
  fflush(stdout);
  return 0;
}

int run_server(struct handle *handle){
  while (1){

      // if there are old requests- handle them
      for (int i = 0; i < RECV_BUFS_AMOUNT; i++){
          if (handle->ctx->pending_request_bufs[i] == 1){
              server_parse_request(handle, i);
              handle->ctx->pending_request_bufs[i] = 0;
            }
        }

      // poll for a new request
      struct ibv_wc wc;
      do {
          if (kv_wait_completion_recv (handle, &wc) == 1) {
              fprintf (stderr, "server couldn't poll for a completion queue item.");
              return 1;
            }
        }
      while (wc.wr_id > MAX_RECV_BUF_INDEX);
      fprintf (stdout, "server got here 1\n");
      fflush(stdout);
      if (server_parse_request(handle, (int) wc.wr_id) == 1){
          fprintf(stderr, "server failed to parse rend get request.\n");
          return 1;
        }
    }
}

int recv_set_requests(struct handle *handle, int iters){
  for (int i = 0; i < iters; ++i){
      struct ibv_wc wc;
      do {
          if (kv_wait_completion_recv (handle, &wc) == 1) {
              fprintf (stderr, "server couldn't poll for a completion queue item.");
              return 1;
            }
        }
      while (wc.wr_id > MAX_RECV_BUF_INDEX);

      if (server_parse_request(handle, (int) wc.wr_id)){
          fprintf(stderr, "Failed to parse a client's set request.");
          return 1;
        }
    }

  return 0;
}


int send_client_set_requests(struct handle *kv_handle, int msg_size, int msg_amount){
  struct timeval t1, t2;

  // create key and value, such that size(key) + size(value) = msg_size - 5
  int val_size = msg_size - SET_KEY_SIZE - SET_PREFIX_SIZE;
  char key[2] = {'a', '\0'} ;
  char* value = (char*) malloc((val_size) * sizeof(char));

  if (value == NULL) {
      printf("Memory allocation failed.\n");
      return 1;
    }

  // Initialize all elements with 'b'
  for (int i = 0; i < val_size - 1; i++) {
      value[i] = 'b';
    }
  value[val_size - 1]= '\0';

  // if not warm up- open clock
  if (msg_amount == DATA_AMOUNT){
      gettimeofday(&t1, NULL);
    }

  if (send_set_requests (kv_handle, key, value, msg_amount) == 1){
      return 1;
    }

  // if not warm up- stop clock and measure throughput
  if (msg_amount == DATA_AMOUNT){
      gettimeofday(&t2, NULL);

      // calculate and print throughput
      double time_taken;
      time_taken = (t2.tv_sec - t1.tv_sec) * 1000000.0; // sec to micro
      time_taken += (t2.tv_usec - t1.tv_usec);

      print_throughput(time_taken, msg_size);
    }

  free(value);
  return 0;
}

int kv_prepare_get_requests(int msg_size, char **value){
  int val_size = msg_size - SET_KEY_SIZE - SET_PREFIX_SIZE;
  *value = (char*) malloc((val_size) * sizeof(char));

  if (*value == NULL) {
      printf("Memory allocation failed.\n");
      return 1;
    }

  // Initialize all elements with 'b'
  for (int i = 0; i < val_size - 1; i++) {
      (*value)[i] = 'b';
    }
  (*value)[val_size - 1]= '\0';
}


int send_client_get_requests(struct handle *kv_handle, int msg_size, int msg_amount){
  struct timeval t1, t2;

  char key[2] = {'a', '\0'} ;

  // set a value of current size
  char *value;
  kv_prepare_get_requests (msg_size, &value);
  kv_set(kv_handle, key, value);

  // wait for completion of set request that prepared the dict in the server
  kv_wait_completions (kv_handle, WC_BATCH);

  // if not warm up- open clock
  if (msg_amount == DATA_AMOUNT){
      gettimeofday(&t1, NULL);
    }

  if (send_get_requests (kv_handle, key, msg_amount) == 1){
      return 1;
    }

  // if not warm up- stop clock and measure throughput
  if (msg_amount == DATA_AMOUNT){
      gettimeofday(&t2, NULL);

      // calculate and print throughput
      double time_taken;
      time_taken = (t2.tv_sec - t1.tv_sec) * 1000000.0; // sec to micro
      time_taken += (t2.tv_usec - t1.tv_usec);

      print_throughput(time_taken, msg_size);
    }

  free(value);
  return 0;
}


int run_eager_client(struct handle *kv_handle){
  // measure kv_set throughput
  fprintf(stdout, "Running eager client with kv_set: \n");
  fflush(stdout);
  size_t msg_size = INIT_MSG_SIZE;
  while (msg_size <= MAX_EAGER_MSG_SIZE){

      send_client_set_requests(kv_handle, msg_size, WARM_UP_AMOUNT);
      send_client_set_requests(kv_handle, msg_size, DATA_AMOUNT);
      msg_size = msg_size << 1;
    }

  // measure kv_get throughput
  fprintf(stdout, "Running eager client with kv_get: \n");
  msg_size = INIT_MSG_SIZE;

  // clean completion queue before starting get benchmark
  struct handle *handle = (struct handle*) kv_handle;
  kv_wait_completions (handle, WC_BATCH);

  while (msg_size <= MAX_EAGER_MSG_SIZE){

      send_client_get_requests(kv_handle, msg_size, WARM_UP_AMOUNT);
      send_client_get_requests(kv_handle, msg_size, DATA_AMOUNT);
      msg_size = msg_size << 1;
    }

  return 0;
}


int recv_get_requests(struct handle *handle, int iters) {
  // receive a single set request for the current value size
  struct ibv_wc wc;
  do {
      if (kv_wait_completion_recv (handle, &wc) == 1) {
          fprintf (stderr, "server couldn't poll for a completion queue item.");
          return 1;
        }
    }
  while (wc.wr_id > MAX_RECV_BUF_INDEX);

  if (server_parse_request(handle, (int) wc.wr_id)){
      fprintf(stderr, "Failed to parse a client's get request.");
      return 1;
    }

  // receive benchmark get requests
  for (int i = 0; i < iters; ++i){
      struct ibv_wc wc;
      do {
          if (kv_wait_completion_recv (handle, &wc) == 1) {
              fprintf (stderr, "server couldn't poll for a completion queue item.");
              return 1;
            }
        }
      while (wc.wr_id > MAX_RECV_BUF_INDEX);

      if (server_parse_request(handle, (int) wc.wr_id)){
          fprintf(stderr, "Failed to parse a client's get request.");
          return 1;
        }
    }

  return 0;
}

int run_eager_server(void *kv_handle){
  struct handle *handle = (struct handle*) kv_handle;

  // receive eager set messages
  int msg_size = INIT_MSG_SIZE;
  while (msg_size <= MAX_EAGER_MSG_SIZE){
      recv_set_requests (handle, WARM_UP_AMOUNT);
      recv_set_requests (handle, DATA_AMOUNT);

      msg_size = msg_size << 1;
    }
  fprintf(stdout, "server finished running eager set requests\n");
  fflush(stdout);
  // receive eager get messages
  msg_size = INIT_MSG_SIZE;
  while (msg_size <= MAX_EAGER_MSG_SIZE){
      recv_get_requests (handle, WARM_UP_AMOUNT);
      recv_get_requests (handle, DATA_AMOUNT);

      msg_size = msg_size << 1;
    }

  return 0;
}


/* Conenct to the server */
int kv_open(char *servername, void **kv_handle){
  struct ibv_device      **dev_list;
  struct ibv_device       *ib_dev;
  struct pingpong_context *ctx;
  struct pingpong_dest     my_dest;
  struct pingpong_dest    *rem_dest;
  char                    *ib_devname = NULL;
  int                      port = 12345;
  int                      ib_port = 1;
  enum ibv_mtu             mtu = IBV_MTU_2048;
  int                      rx_depth = 100;
  int                      tx_depth = 10;
  int                      iters = 10000;
  int                      use_event = 0;
  int                      size =  MEGABYTE;
  int                      sl = 0;
  int                      gidx = -1;
  char                     gid[33];

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

      char **argv;
      c = getopt_long(1, argv, "p:d:i:s:m:r:n:l:eg:", long_options, NULL);

      if (c == -1)
        break;

      switch (c) {
          case 'p':
            port = strtol(optarg, NULL, 0);
          if (port < 0 || port > 65535) {
              usage();
              return 1;
            }
          break;

          case 'd':
            ib_devname = strdup(optarg);
          break;

          case 'i':
            ib_port = strtol(optarg, NULL, 0);
          if (ib_port < 0) {
              usage();
              return 1;
            }
          break;

          case 's':
            size = strtol(optarg, NULL, 0);
          break;

          case 'm':
            mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
          if (mtu < 0) {
              usage();
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
            usage();
          return 1;
        }
    }

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

  if (ctx->pd == NULL){
      ctx = kv_init_ctx (ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername);
      if (!ctx)
        return 1;
    }

//  ctx->routs = pp_post_recv(ctx, ctx->rx_depth);
//  if (ctx->routs < ctx->rx_depth) {
//      fprintf(stderr, "Couldn't post receive (%d)\n", ctx->routs);
//      return 1;
//    }

  if (use_event)
    if (ibv_req_notify_cq(ctx->cq, 0)) {
        fprintf(stderr, "Couldn't request CQ notification\n");
        return 1;
      }

  if (kv_get_port_info (ctx->context, ib_port, &ctx->portinfo)) {
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
  printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
         my_dest.lid, my_dest.qpn, my_dest.psn, gid);

  if (servername)
    rem_dest = kv_client_exch_dest (servername, port, &my_dest);
  else
    rem_dest = kv_server_exch_dest (ctx, ib_port, mtu, port, sl, &my_dest, gidx);

  if (!rem_dest)
    return 1;

  inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
  printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
         rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

  if (servername)
    if (kv_connect_ctx (ctx, ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
      return 1;

  struct handle *handle = calloc(1, sizeof(struct handle));
  *kv_handle = handle;
  handle->dev_list = dev_list;
  handle->rem_dest = rem_dest;
  handle->ctx = ctx;

  // mark first 5 buffers as used and use them to post_recv
  for (int i = 0; i < RECV_BUFS_AMOUNT; i++){
      if (kv_post_recv(handle, i)){
          fprintf(stderr, "Failed to post receive.");
          return 1;
        }
      handle->ctx->free_set_bufs_amount--;
      handle->ctx->free_bufs[i] = 0;
      handle->ctx->pending_request_bufs[i] = 0;
    }

  hashmap* map = hashmap_create();
  handle->map = map;

  *kv_handle = handle;
  return 0;
}


/* Called after get() on value pointer */
void kv_release(char *value){
  free(value);
}


/* Destroys the QP */
int kv_close(void *kv_handle)
{
  struct handle *handle = (struct handle*) kv_handle;
  ibv_free_device_list(handle->dev_list);
  free(handle->rem_dest);
  hashmap_free(handle->map);
  free(handle);
  return 0;
}

int run_part_1(const char *servername, struct handle *kv_handle){
  fprintf(stdout, "Running Part 1...");
  fflush(stdout);
  if (servername) { // client
      if (run_eager_client(kv_handle) == 1){
          return 1;
        }
      printf("Part 1- eager client done.\n");
    } else { //server
      if (run_eager_server(kv_handle) == 1){
          return 1;
        }
      printf("Part 1- eager server done.\n");
    }
}

int run_part_2(const char *servername, struct handle *kv_handle){
  fprintf(stdout, "Running Part 2...\n");
  fflush(stdout);

  fprintf(stdout, "Running small rend set test: client does rend set. server receives a rend set, performs hashmap_set and and prints the value he read.\n");
  fflush(stdout);
  if (servername) { // client
      char key[2] = {'a', '\0'};
      char *big_value = (char *) malloc(sizeof(char) * 5000);
      for (int i = 0; i < 4999; i++){
          big_value[i] = 'b';
        }
      big_value[4999] = '\0';
      if (kv_set(kv_handle, key, big_value) == 1){
          fprintf(stderr, "client failed to rend set.\n");
          return 1;
        }

      free(big_value);
      printf("Part 2- small test rend set client done.\n");

    } else { //server
      struct ibv_wc wc;
      do {
          if (kv_wait_completion_recv (kv_handle, &wc) == 1) {
              fprintf (stderr, "server couldn't poll for a completion queue item.");
              return 1;
            }
        }
      while (wc.wr_id > MAX_RECV_BUF_INDEX);
      if (server_parse_request(kv_handle, (int) wc.wr_id) == 1){
          fprintf(stderr, "server failed to parse rend set request.\n");
          return 1;
        }

      uintptr_t cur_val;
      hashmap_get(kv_handle->map, "a", 2, &cur_val);
      fprintf(stdout, "current value is: %s\n", (char *)cur_val);
      fprintf(stdout, "length is: %lu\n", strlen((char*)cur_val));

      printf("Part 2- small test rend set server done.\n");
    }

  fprintf(stdout, "Running small rend get test: client sends get. server receives a get, performs rend get, and clients prints the value he got.\n");
  fflush(stdout);

  if (servername) { // client
      char key[2] = {'a', '\0'};
      char *big_value = (char *) malloc(sizeof(char) * 5000);
      if (kv_get(kv_handle, key, &big_value) == 1){
          fprintf(stderr, "client failed to rend get.\n");
          return 1;
        }

      free(big_value);
      printf("Part 2- small test rend get client done. value is: %s\n", big_value);

    } else { //server
      struct ibv_wc wc;
      do {
          if (kv_wait_completion_recv (kv_handle, &wc) == 1) {
              fprintf (stderr, "server couldn't poll for a completion queue item.");
              return 1;
            }
        }
      while (wc.wr_id > MAX_RECV_BUF_INDEX);
      if (server_parse_request(kv_handle, (int) wc.wr_id) == 1){
          fprintf(stderr, "server failed to parse rend get request.\n");
          return 1;
        }

      printf("Part 2- small test rend get server done.\n");
    }
}

int run_part_3(const char *servername, struct handle *kv_handle, char *argv[] ,int argc){
  fprintf(stdout, "Running Part 3...\n");
  fflush(stdout);
  if (servername) { // client
      if (argc < 2){ // no input file
          return 0;
        }

      char operation[4];
      char *key = malloc(sizeof(char) * MAX_EAGER_MSG_SIZE);
      if (!key){
          fprintf(stderr, "failed to allocate memory for key.\n");
          return 1;
        }

      FILE* input_file = fopen(argv[2], "r");
      if (input_file == NULL) {
          printf("Error opening the input file.\n");
          return 1;
        }

      while (fscanf(input_file, "%s", operation) != EOF) {
          if (strcmp(operation, "GET") == 0) {
              char *value;
              fscanf(input_file, "%s", key);
              fprintf(stdout, "got here 1\n");
              fflush(stdout);
              if (kv_get(kv_handle, key, &value)){
                  fprintf(stderr, "failed to get value for key.\n");
                  return 1;
                }
              printf("GET %s: %s\n", key, value);
              kv_release(value);
            }

          else if (strcmp(operation, "SET") == 0) {
              char *value = malloc(sizeof(char) * MEGABYTE);
              if (!value){
                  fprintf(stderr, "failed to allocate memory for value.\n");
                  return 1;
                }
              fscanf(input_file, "%s %s", key, value);
              printf("SET %s %s\n", key, value);
              if(kv_set(kv_handle, key, value)){
                  fprintf(stderr, "failed to set key-value pair.\n");
                  return 1;
                }
              kv_release(value);
            } else {
              printf("Invalid operation: %s\n", operation);
            }
        }

      free(key);

    } else { //server
      if (run_server(kv_handle) == 1){
          fprintf(stderr, "server failed to parse rend get request.\n");
          return 1;
        }
    }
}



int main(int argc, char *argv[])
{
  char *servername = NULL;

  srand48(getpid() * time(NULL));

  if (optind == argc - 1 || optind == argc - 2)
    servername = strdup(argv[optind]);
  else if (optind < argc) {
      usage();
      return 1;
    }


  struct handle *kv_handle;
  if (servername){ //client
      if (kv_open(servername, (void **) &kv_handle)){
          fprintf(stderr, "Failed to connect.");
          return 1;
        }
    }
  else { // server
      int clients_amount = TWO_CLIENTS;
      for (int i = 0; i < clients_amount; i++){
          if (kv_open(servername, (void **) &kv_handle)){
              fprintf(stderr, "Failed to connect.");
              return 1;
            }
        }
    }


//  run_part_1(servername, kv_handle);
//  run_part_2(servername, kv_handle);
  run_part_3(servername, kv_handle, argv, argc);

  kv_close(kv_handle);
  return 0;
}
