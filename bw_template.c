//------------------------------------------------------------
// -------------------- INCLUDES -----------------------------
//------------------------------------------------------------
#include <math.h>
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
#include <sys/param.h>
#include <infiniband/verbs.h>
#include "tests.c"


//------------------------------------------------------------
// -------------------- MAGIC NUMBERS -----------------------------
//------------------------------------------------------------
#define MSG_INIT_SIZE 1              // Initial size of a message in bytes.
#define WC_BATCH (1)                 //Batch size for RDMA Work Completions.
#define ALL_MESSAGES_SIZE (1048576L) //Total size of all messages in bytes.
#define NUM_CLIENT 1                 //Number of clients.
#define SET 0                        // Constants to identify SET request types.
#define GET 1                        // Constants to identify GET request types.
#define RENDEZVOUS 1                 //Constants to determine the protocol RDV
#define EAGER 0                      //Constants to determine the protocol Eager
#define MAX_REQUEST 5                //Maximum number of requests.
#define INIT_KEY 4096


static int page_size;
int argc_init;
char **argv_init;


enum {
    PINGPONG_RECV_WRID = 1, //Identifier for receiving a Work Request.
    PINGPONG_SEND_WRID = 2, //Identifier for sending a Work Request.
};

/*
 * This structure represents the necessary information for the RDV protocol.
 */
struct Rendezvous {
    void *addr;         // Address of the remote data.
    unsigned long size; // Size of the remote data in bytes.
    uint32_t rkey;      // Rendezvous key for remote access permissions.
};

/**
 * Struct representing a data entry in the key-value store.
 */
struct data {
    char key[INIT_KEY];              // Key for the key-value store
    char eager_val[INIT_KEY];        // Value for "eager" transfer protocol
    char *rdv_val;               // Pointer for "Rendezvous" transfer protocol
    int request;                 // Type of request (SET or GET)
    int protocol;                // Protocol type (Rendezvous or Eager)
    struct Rendezvous rdv_get;   // Information about remote data for GET under Rendezvous
    struct Rendezvous rdv_set;   // Information about remote data for SET under Rendezvous
};

//------------------------------------------------------------
// -------------------- KV STORE -----------------------------
//------------------------------------------------------------
/**
 * Struct representing the key-value store (kv_store).
 */
struct kv_store {
    struct data **datas;    // Array of data pointers.
    int size;               // Current number of elements in the kv_store.
    int capacity;           // Maximum number of elements the kv_store can hold.
};


/**
 * Increase the capacity of the key-value store if it's full.
 * @param kv_store Pointer to the kv_store structure.
 */
void increase_kv_store_capacity(struct kv_store *kv_store) {
  if ((kv_store->capacity) == kv_store->size) {

      // Double the capacity.
      kv_store->capacity *= 2;

      // Allocate a new array with the increased capacity.
      struct data **new_datas = malloc(sizeof(struct data*) * kv_store->capacity);

      // Copy existing data pointers to the new array.
      for (int i = 0; i < kv_store->size; ++i) {
          new_datas[i] = kv_store->datas[i];
        }

      // Initialize the newly allocated elements to NULL.
      for (int i = kv_store->size; i < kv_store->capacity; ++i) {
          new_datas[i] = malloc(sizeof (struct data));
        }

      // Update the kv_store's data array to the new one.
      kv_store->datas = new_datas;
    }
}



//------------------------------------------------------------
// -------------------- PINGPONG -----------------------------
//------------------------------------------------------------

/**
 * Struct representing a context for the pingpong application.
 */
struct pingpong_context {
    struct ibv_context		*context;      // InfiniBand context.
    struct ibv_comp_channel	*channel;      // Completion channel.
    struct ibv_pd		*pd;               // Protection Domain.
    struct ibv_mr		*mr[MAX_REQUEST];  // Memory Regions for RDMA.
    struct ibv_cq		*cq;               // Completion Queue.
    struct ibv_qp		*qp;               // Queue Pair.
    void			*buf[MAX_REQUEST];     // Buffers for data.
    unsigned long		size;              // Size of the buffer.
    int				rx_depth;              // Depth of receive operations.
    int				routs;                 // Number of outstanding Work Requests.
    struct ibv_port_attr	portinfo;      // Port information.
    size_t current_buffer;                 // Index of the current buffer in use.
};

/**
 * Struct representing destination information for a pingpong application.
 */
struct pingpong_dest {
    int lid;              // Local Identifier (LID) of the destination.
    int qpn;              // Queue Pair Number (QPN) of the destination.
    int psn;              // Packet Sequence Number (PSN) of the destination.
    union ibv_gid gid;    // Global Identifier (GID) of the destination.
};

/**
 * Convert an MTU value to the corresponding enum value.
 * @param mtu The MTU value to convert.
 * @return The corresponding enum ibv_mtu value.
 */
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

/**
 * Get the Local Identifier (LID) for a specific port on a given InfiniBand context.
 * @param context The InfiniBand context.
 * @param port The port for which to retrieve the LID.
 * @return The Local Identifier (LID) for the specified port, or 0 if an error occurs.
 */
uint16_t pp_get_local_lid(struct ibv_context *context, int port)
{
  struct ibv_port_attr attr;

  if (ibv_query_port(context, port, &attr))
    return 0;

  return attr.lid;
}

/**
 * Get the attributes of a specific port on a given InfiniBand context.
 * @param context The InfiniBand context.
 * @param port The port for which to retrieve the attributes.
 * @param attr Pointer to a struct to store the port attributes.
 * @return 0 on success, or an error code if the query fails.
 */
int pp_get_port_info(struct ibv_context *context, int port,
                     struct ibv_port_attr *attr)
{
  return ibv_query_port(context, port, attr);
}

/**
 * Convert a wire representation of GID to an ibv_gid data structure.
 * @param wgid The wire representation of GID (hexadecimal string).
 * @param gid Pointer to an ibv_gid union to store the converted GID.
 */
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

/**
 * Convert an ibv_gid data structure to its wire representation (hexadecimal string).
 * @param gid Pointer to an ibv_gid union representing the GID.
 * @param wgid Character array to store the wire representation.
 */
void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
  int i;

  for (i = 0; i < 4; ++i)
    sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *)(gid->raw + i * 4)));
}

/**
 * Connect the local context to a remote context using a Queue Pair (QP).
 * @param ctx Pointer to the pingpong_context structure.
 * @param port Port number to use for the connection.
 * @param my_psn Packet Sequence Number (PSN) of the local context.
 * @param mtu Maximum Transmission Unit (MTU) for the connection.
 * @param sl Service Level (SL) for the connection.
 * @param dest Pointer to the pingpong_dest structure representing the remote context.
 * @param sgid_idx Index of the Source Global Identifier (SGID).
 * @return 0 on success, 1 on failure.
 */
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
/**
 * Exchange destination information between a client and a server using sockets.
 * @param servername The server's hostname.
 * @param port Port number for the socket connection.
 * @param my_dest Pointer to the local pingpong_dest structure.
 * @return Pointer to the remote pingpong_dest structure on success, or NULL on failure.
 */
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
/**
 * Exchange destination information between a server and a client using sockets.
 * @param ctx Pointer to the pingpong_context structure.
 * @param ib_port InfiniBand port to use.
 * @param mtu Maximum Transmission Unit (MTU) for the connection.
 * @param port Port number for the socket connection.
 * @param sl Service Level (SL) for the connection.
 * @param my_dest Pointer to the local pingpong_dest structure.
 * @param sgid_idx Index of the Source Global Identifier (SGID).
 * @return Pointer to the remote pingpong_dest structure on success, or NULL on failure.
 */
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
/**
* Initializes a new pingpong_context structure.
* @param ib_dev Pointer to the InfiniBand device.
* @param size Size of buffers for communication operations.
* @param rx_depth Depth of receive operations.
* @param tx_depth Depth of send operations.
* @param port Port number to use.
* @param use_event Flag to enable event notifications.
* @param is_server Flag indicating whether it's a server context.
* @return Pointer to the initialized pingpong_context structure, or NULL if an error occurs.
*/
static struct pingpong_context *pp_init_ctx(struct ibv_device *ib_dev, int size,
                                            int rx_depth, int tx_depth, int port,
                                            int use_event, int is_server)
{
  struct pingpong_context *ctx;
  // Allocate memory for the context.
  ctx = calloc(1, sizeof *ctx);
  if (!ctx)
    return NULL;

  // Initialize the context parameters.
  ctx->size     = size;
  ctx->rx_depth = rx_depth;
  ctx->routs    = rx_depth;

  // Allocate buffers for communication operations.
  for (int i = 0; i < MAX_REQUEST; i++){
      ctx->buf[i] = malloc(roundup(size, page_size));
      if (!ctx->buf[i]) {
          fprintf(stderr, "Couldn't allocate work buf.\n");
          return NULL;
        }
      memset(ctx->buf[i], 0x7b + is_server, size);
    }


  // Open the InfiniBand device and obtain the context.
  ctx->context = ibv_open_device(ib_dev);
  if (!ctx->context) {
      fprintf(stderr, "Couldn't get context for %s\n",
              ibv_get_device_name(ib_dev));
      return NULL;
    }
  // Create a completion channel for event notifications, if enabled.
  if (use_event) {
      ctx->channel = ibv_create_comp_channel(ctx->context);
      if (!ctx->channel) {
          fprintf(stderr, "Couldn't create completion channel\n");
          return NULL;
        }
    } else
    ctx->channel = NULL;

  // Allocate a Protection Domain (PD).
  ctx->pd = ibv_alloc_pd(ctx->context);
  if (!ctx->pd) {
      fprintf(stderr, "Couldn't allocate PD\n");
      return NULL;
    }
  // Register memory buffers for RDMA (Remote Direct Memory Access) operations.
  for(int i = 0 ; i < MAX_REQUEST ; i++){
      ctx->mr[i] = ibv_reg_mr(ctx->pd, ctx->buf[i], size, IBV_ACCESS_LOCAL_WRITE);
      if (!ctx->mr[i]) {
          fprintf(stderr, "Couldn't register MR\n");
          return NULL;
        }
    }

  // Create a Completion Queue (CQ).
  ctx->cq = ibv_create_cq(ctx->context, rx_depth + tx_depth, NULL,
                          ctx->channel, 0);
  if (!ctx->cq) {
      fprintf(stderr, "Couldn't create CQ\n");
      return NULL;
    }

  // Create a Queue Pair (QP).
  {
    struct ibv_qp_init_attr attr = {
        .send_cq = ctx->cq,
        .recv_cq = ctx->cq,
        .cap     = {
            .max_send_wr  = tx_depth,
            .max_recv_wr  = rx_depth,
            .max_send_sge = 1,
            .max_recv_sge = 1,
            .max_inline_data = 64
        },
        .qp_type = IBV_QPT_RC
    };

    ctx->qp = ibv_create_qp(ctx->pd, &attr);
    if (!ctx->qp)  {
        fprintf(stderr, "Couldn't create QP\n");
        return NULL;
      }
  }

  // Modify the QP state to INIT (Initialization).
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

//------------------------------------------------------------
// -------------------- PP FUNCTIONS -------------------------
//------------------------------------------------------------

/**
 * Posts a send operation to the Queue Pair.
 * @param ctx Pointer to the pingpong_context structure.
 * @param lptr Local pointer to the send buffer.
 * @param rptr Remote pointer to the destination buffer (for RDMA write).
 * @param rkey Remote key for the destination buffer.
 * @param opcode Operation code for the send.
 * @return 0 on success, -1 on failure.
 */
static int pp_post_send(struct pingpong_context *ctx ,const char *lptr, void *rptr, uint32_t rkey,enum ibv_wr_opcode opcode)
{
  uintptr_t addr;
  // Determine the source address for the send operation.
  if(lptr){
      addr=(uintptr_t) lptr;
    }
  else{
      addr = (uintptr_t) ctx->buf[ctx->current_buffer];
    }
  // Create a scatter-gather element (SGE) representing the send buffer.
  struct ibv_sge list = {
      .addr	= addr,
      .length = ctx->size,
      .lkey	= ctx->mr[ctx->current_buffer]->lkey
  };
  // Create a send work request.
  struct ibv_send_wr *bad_wr, wr = {
      .wr_id	    = PINGPONG_SEND_WRID,
      .sg_list    = &list,
      .num_sge    = 1,
      .opcode     = opcode,
      .send_flags = IBV_SEND_SIGNALED,
      .next       = NULL
  };
  // If it's an RDMA write operation, set the remote address and key.
  if(rptr){
      wr.wr.rdma.remote_addr= (uintptr_t) rptr;
      wr.wr.rdma.rkey=rkey;
    }
  // Post the send work request to the Queue Pair.
  return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

/**
 * Waits for completions on the Completion Queue (CQ) for a specified number of iterations.
 * @param ctx Pointer to the pingpong_context structure.
 * @param iters Number of iterations to wait for completions.
 * @return 0 if successful, 1 if an error occurs.
 */
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

/**
 * Closes and releases the resources associated with the pingpong_context.
 * @param ctx Pointer to the pingpong_context structure.
 * @return 0 if successful, 1 if an error occurs.
 */
int pp_close_ctx(struct pingpong_context *ctx)
{
  // Destroy the Queue Pair (QP).
  if (ibv_destroy_qp(ctx->qp)) {
      fprintf(stderr, "Couldn't destroy QP\n");
      return 1;
    }
  // Destroy the Completion Queue (CQ).
  if (ibv_destroy_cq(ctx->cq)) {
      fprintf(stderr, "Couldn't destroy CQ\n");
      return 1;
    }
  // Deregister Memory Regions (MRs).
  for(int i = 0 ; i < MAX_REQUEST; i++){
      if (ibv_dereg_mr(ctx->mr[i])) {
          fprintf(stderr, "Couldn't deregister MR\n");
          return 1;
        }
    }

  // Deallocate the Protection Domain (PD).
  if (ibv_dealloc_pd(ctx->pd)) {
      fprintf(stderr, "Couldn't deallocate PD\n");
      return 1;
    }

  // Destroy the completion channel, if it was used.
  if (ctx->channel) {
      if (ibv_destroy_comp_channel(ctx->channel)) {
          fprintf(stderr, "Couldn't destroy completion channel\n");
          return 1;
        }
    }
  // Close the InfiniBand device context.
  if (ibv_close_device(ctx->context)) {
      fprintf(stderr, "Couldn't release context\n");
      return 1;
    }
  // Free the allocated memory for buffers and the context itself.
  for(int i = 0; i < MAX_REQUEST ; i++){
      free(ctx->buf[i]);
    }
  free(ctx);

  return 0;
}

/**
 * Posts a specified number of receive work requests to the receive queue of the context.
 * @param ctx Pointer to the pingpong_context structure.
 * @param n Number of receive work requests to post.
 * @return Number of successfully posted work requests.
 */
static int pp_post_recv(struct pingpong_context *ctx, int n)
{
  // Prepare the scatter-gather element (SGE) list for the receive work request.
  struct ibv_sge list = {
      .addr	= (uintptr_t) ctx->buf[ctx->current_buffer],  // Starting address of the buffer
      .length = ctx->size, // Size of the data to be received
      .lkey	= ctx->mr[ctx->current_buffer]->lkey // Local key of the memory region
  };
  // Initialize the receive work request.
  struct ibv_recv_wr wr = {
      .wr_id	    = PINGPONG_RECV_WRID, // Identifier for this work request
      .sg_list    = &list,// List of scatter-gather elements
      .num_sge    = 1,
      .next       = NULL
  };
  struct ibv_recv_wr *bad_wr;
  int i;
  for (i = 0; i < n; ++i)
    // Attempt to post a receive work request to the queue.
    // If successful, the work request is added to the receive queue.
    if (ibv_post_recv(ctx->qp, &wr, &bad_wr)) {
        break;
      }
  return i; // Return the number of successfully posted work requests.
}


/**
 * Initialize a connection to the server.
 * @param servername The name of the server.
 * @param kv_handle Pointer to store the initialized pingpong_context structure.
 * @return 0 if connection is successfully initialized, 1 if there's an error.
 */
int initialize_connection(char * servername, struct pingpong_context **kv_handle) {
  struct ibv_device **dev_list;
  struct ibv_device *ib_dev;
  struct pingpong_context *ctx;
  struct pingpong_dest my_dest;
  struct pingpong_dest *rem_dest;
  char *ib_devname = NULL;
  int port = 2510;
  int ib_port = 1;
  enum ibv_mtu mtu = IBV_MTU_2048;
  int rx_depth = 5000;
  int tx_depth = 5000;
  int iters = 50000;
  int use_event = 0;
  int size = ALL_MESSAGES_SIZE;
  int sl = 0;
  int gidx = -1;
  char gid[33];

  srand48(getpid() * time(NULL));

  while (1) {
      int c;

      static struct option long_options[] = {
          {.name = "port", .has_arg = 1, .val = 'p'},
          {.name = "ib-dev", .has_arg = 1, .val = 'd'},
          {.name = "ib-port", .has_arg = 1, .val = 'i'},
          {.name = "size", .has_arg = 1, .val = 's'},
          {.name = "mtu", .has_arg = 1, .val = 'm'},
          {.name = "rx-depth", .has_arg = 1, .val = 'r'},
          {.name = "iters", .has_arg = 1, .val = 'n'},
          {.name = "sl", .has_arg = 1, .val = 'l'},
          {.name = "events", .has_arg = 0, .val = 'e'},
          {.name = "gid-idx", .has_arg = 1, .val = 'g'},
          {0}
      };

      c = getopt_long(argc_init, argv_init, "p:d:i:s:m:r:n:l:eg:", long_options, NULL);
      if (c == -1)
        break;

      switch (c) {
          case 'p':
            port = strtol(optarg, NULL, 0);
          if (port < 0 || port > 65535) {
              usage(argv_init[0]);
              return 1;
            }
          break;

          case 'd':
            ib_devname = strdup(optarg);
          break;

          case 'i':
            ib_port = strtol(optarg, NULL, 0);
          if (ib_port < 0) {
              usage(argv_init[0]);
              return 1;
            }
          break;

          case 's':
            size = strtol(optarg, NULL, 0);
          break;

          case 'm':
            mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
          if (mtu < 0) {
              usage(argv_init[0]);
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
            usage(argv_init[0]);
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

  ctx = pp_init_ctx(ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername);
  if (!ctx)
    return 1;

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
  printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
         my_dest.lid, my_dest.qpn, my_dest.psn, gid);


  if (servername)
    rem_dest = pp_client_exch_dest(servername, port, &my_dest);
  else
    rem_dest = pp_server_exch_dest(ctx, ib_port, mtu, port, sl, &my_dest, gidx);

  if (!rem_dest)
    return 1;

  inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
  printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
         rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

  if (servername)
    if (pp_connect_ctx(ctx, ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
      return 1;
  *kv_handle = ctx;
  ibv_free_device_list(dev_list);
  free(rem_dest);
  return 0;
}


/**
 * Sends a message from the server to the client.
 * @param ctx The pingpong context.
 * @return 0 on success, 1 on failure.
 */
int send_data(struct pingpong_context *ctx) {
  // Set the size of the data to be sent.
  ctx->size = sizeof(struct data);

  // Post a send operation to the queue.
  if (pp_post_send(ctx, NULL, NULL, 0, IBV_WR_SEND)) {
      printf("Error send data");
      return 1;
    }
  // Wait for the send operation to complete.
  if (pp_wait_completions(ctx, 1)) {
      printf("Error waiting completions");
      return 1;
    }
  return 0;
}

/**
 * Receives a message from the client to the server.
 * @param ctx The pingpong context.
 * @param size The size of the data to be received.
 * @return 0 on success, 1 on failure.
 */
int receive_data(struct pingpong_context *ctx, size_t size) {
  // Set the size of the data to be received.
  ctx->size = size;

  // Post a receive operation to the queue.
  if (pp_post_recv(ctx, 1) != 1) {
      printf("Error receive data");
      return 1;
    }

  // Wait for the receive operation to complete.
  if (pp_wait_completions(ctx, 1)) {
      printf("Error waiting completions");
      return 1;
    }
  return 0;
}

//------------------------------------------------------------
// -------------------- KV FUNCTIONS -------------------------
//------------------------------------------------------------

/**
 * Open a connection to the server and initialize the kv_handle.
 * @param servername The server's hostname.
 * @param kv_handle Pointer to the kv_handle to be initialized with the context.
 * @return 0 on success, or an error code on failure.
 */
int kv_open(char *servername, void ** kv_handle) {
  return initialize_connection(servername, (struct pingpong_context ** )kv_handle);
}

/**
 * Sets a key-value pair in the key-value store using the specified protocol (Eager or Rendezvous).
 * If the value is smaller than 4KB, Eager protocol is used, otherwise Rendezvous protocol is used.
 *
 * @param kv_handle The handle to the pingpong_context representing the client.
 * @param key The key for the key-value pair.
 * @param value The value associated with the key.
 * @return 0 on success, 1 on error.
 */
int kv_set(void *kv_handle, const char *key, const char *value) {
  struct pingpong_context *ctx = (struct pingpong_context*) kv_handle;
  unsigned data_size = strlen(key) + strlen(value);
  // Get a reference to the data buffer in the context
  struct data *data = (struct data*)ctx->buf[ctx->current_buffer];
  data->request = SET;
  // Check if key + value < 4 KB
  if (data_size < 4096) {
      // EAGER PROTOCOL
      ctx->size = sizeof (struct data);
      data->protocol = EAGER;
      // Copy the key and value into the data struct
      memcpy(data->key, key, sizeof(data->key));
      memcpy(data->eager_val, value, sizeof(data->eager_val));
      // Send the data using send_data function
      if (send_data(ctx)) {
          return 1;
        }
    }
  else {
      data->protocol = RENDEZVOUS;
      size_t size_value = strlen(value) + 1;
      data->rdv_set.size = size_value;
      memcpy(data->key, key, sizeof(data->key));
      //on envoie une demande au server de preparer un buffer pour qu il recoive la nouvelle value
      if (send_data(ctx) || receive_data(ctx, sizeof(struct data))) {
          return 1;
        }
      struct data *pack_response = (struct data*)ctx->buf[ctx->current_buffer];
      // Store the current context's memory region for later restoration
      struct ibv_mr* ctxMR = (struct ibv_mr*)ctx->mr[ctx->current_buffer];
      // Cree memory region virtuel pour etre sur que le client peut ecrire la value sur la memory region du server
      // Copie la value de la memory region ctxMR vers la region memoire sur server avec pp_post_send
      struct ibv_mr* clientMR = ibv_reg_mr(ctx->pd, (char *) value,
                                           size_value, IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);

      ctx->mr[ctx->current_buffer] = clientMR;
      ctx->size = size_value;
      // Perform an RDMA write using pp_post_send
      pp_post_send(ctx,
                   value,
                   pack_response->rdv_set.addr,
                   pack_response->rdv_set.rkey,
                   IBV_WR_RDMA_WRITE);
      // Wait for completion of the RDMA write
      if(pp_wait_completions(ctx, 1)) {
          printf("%s", "Error completions");
          return 1;
        };
      ctx->mr[ctx->current_buffer] = (struct ibv_mr*) ctxMR;
      ctx->size = 1;
      // Send an acknowledgement message using pp_post_send
      if (pp_post_send(ctx, NULL, NULL, 0, IBV_WR_SEND)) {
          printf("%d%s", 1, "Error server send");
          return 1;
        }
      if(pp_wait_completions(ctx, 1)) {
          printf("%s", "Error completions");
          return 1;
        }
      ibv_dereg_mr(clientMR);

    }
  // Update the current buffer index for the context
  ctx->current_buffer = (ctx->current_buffer + 1) % MAX_REQUEST;
  return 0;
}


/**
 * Retrieves the value associated with a key from the key-value store.
 * @param kv_handle The handle to the key-value store context.
 * @param key The key to retrieve the value for.
 * @param value A pointer to the variable that will store the retrieved value.
 * @return 0 on success, 1 on error.
 */
int kv_get(void *kv_handle, const char *key, char **value)
{
  struct pingpong_context *ctx = (struct pingpong_context*) kv_handle;
  struct data *data = (struct data*)ctx->buf[ctx->current_buffer];
  // Prepare GET request data
  data->request=GET;
  memcpy(data->key, key, sizeof(data->key));
  // Send GET request and receive response
  if (send_data(ctx) || receive_data(ctx, sizeof(struct data))){
      return 1;
    }
  // Process response
  if(data->protocol==EAGER){
      // EAGER PROTOCOL: Copy the eager_val to the value buffer
      unsigned int value_size=strlen(data->eager_val);
      *value = malloc(value_size+1);
      memcpy(*value, data->eager_val, value_size + 1);
    }
  else{
      // RENDEZVOUS PROTOCOL: Allocate memory for the value and perform RDMA READ
      // Allocate memory to store the value received through RDMA READ
      *value = malloc( data->rdv_get.size);

      // Save the current MR and create a new MR for the value buffer
      struct ibv_mr* save_ctx = ctx->mr[ctx->current_buffer];
      struct ibv_mr* mr_temp = ibv_reg_mr(ctx->pd,*value, data->rdv_get.size,
                                          IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
      ctx->mr[ctx->current_buffer] = mr_temp;
      // Set the size of data to be transferred
      ctx->size = data->rdv_get.size ;
      // Perform RDMA READ operation
      if(pp_post_send(ctx,*value,data->rdv_get.addr, data->rdv_get.rkey,IBV_WR_RDMA_READ)){
          printf("fail to send to server read");
          return 1;
        }
      if(pp_wait_completions(ctx, 1)) {
          printf("RDMA READ failed");
          return 1;
        }
      // Restore the original MR and clean up the temporary MR
      ctx->mr[ctx->current_buffer] = (struct ibv_mr*) save_ctx;
      ibv_dereg_mr(mr_temp);
    }
  ctx->current_buffer = (ctx->current_buffer+1)% MAX_REQUEST;
  return 0;
}

/**
 * Frees the memory allocated for the value obtained from kv_get().
 * @param value The pointer to the value obtained from kv_get().
 */
void kv_release(char *value){
  free(value);
}

/**
 * Closes the connection and releases resources associated with the key-value store handle.
 * @param kv_handle The handle to the key-value store context.
 * @return 0 on success, 1 on error.
 */
int kv_close(void *kv_handle) {
  return pp_close_ctx(kv_handle);
}

//------------------------------------------------------------
// -------------------- SERVER   -----------------------------
//------------------------------------------------------------

/**
 * Handle SET requests received by the server.
 * @param ctx The pingpong context.
 * @param data The received data structure.
 * @param kv_store The key-value store.
 * @param id The buffer ID.
 * @return 0 on success, 1 on failure.
 */
int set_request_server(struct pingpong_context *ctx, struct data *data, struct kv_store *kv_store, size_t id) {
  // Increase kv_store capacity if needed
  increase_kv_store_capacity(kv_store);

  // Check if the key already exists in the kv_store
  for (int i = 0; i < kv_store->size; ++i) {
      if (strcmp(kv_store->datas[i]->key, data->key) == 0) {
          // Key found, update the value based on the protocol
          if (data->protocol == EAGER) {
              memcpy(kv_store->datas[i]->eager_val, data->eager_val, sizeof(data->eager_val));
              return 0;
            } else {
              // RDV protocol
              struct data * res = (struct data*) ctx->buf[id];
              kv_store->datas[i]->rdv_val = calloc(data->rdv_set.size, 1);
              kv_store->datas[i]->protocol = RENDEZVOUS;
              struct ibv_mr* mr = ibv_reg_mr(ctx->pd,  kv_store->datas[i]->rdv_val, data->rdv_set.size,
                                             IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
              res->rdv_set.rkey = mr->rkey;
              res->rdv_set.addr = mr->addr;
              res->request = SET;
              res->protocol = RENDEZVOUS;
              ctx->current_buffer = id;
              if(send_data(ctx)||receive_data(ctx, 1)){
                  return 1;
                }
              return 0;

            }
        }
    }

  // If the key does not exist in the kv_store, create a new entry.
  struct data *new_data = kv_store->datas[kv_store->size];
  memcpy(new_data->key, data->key, sizeof(new_data->key));

  if (data->protocol == EAGER) {
      memcpy(new_data->eager_val, data->eager_val, sizeof(new_data->eager_val));
    } else {
      struct data * res = (struct data*) ctx->buf[id];
      new_data->rdv_val = calloc(data->rdv_set.size, 1);
      new_data->protocol = RENDEZVOUS;
      struct ibv_mr* mr = ibv_reg_mr(ctx->pd, new_data->rdv_val, data->rdv_set.size,
                                     IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
      res->rdv_set.rkey = mr->rkey;
      res->rdv_set.addr = mr->addr;
      res->request = SET;
      res->protocol = RENDEZVOUS;
      ctx->current_buffer = id;
      if(send_data(ctx)||receive_data(ctx, 1)){
          return 1;
        }
    }
  // Increase the kv_store size and return success
  kv_store->size++;
  return 0;
}

/**
 * Handles a GET request from the client in the server.
 * @param ctx The pingpong context.
 * @param data The received data from the client.
 * @param kv_store The key-value store.
 * @param id The buffer index.
 * @return 0 on success, 1 on failure.
 */
int server_handle_get_request(struct pingpong_context *ctx, struct data *data, struct kv_store *kv_store, size_t id) {
  struct data *pack_response = (struct data*)ctx->buf[id];
  for (int i = 0; i < kv_store->size; i++) {
      if(strcmp(kv_store->datas[i]->key, data->key) == 0){
          if (kv_store->datas[i]->protocol == RENDEZVOUS) {
              // RENDEZVOUS PROTOCOL
              size_t value_size = strlen(kv_store->datas[i]->rdv_val) + 1;
              pack_response->protocol = RENDEZVOUS;
              struct ibv_mr* mr_create = ibv_reg_mr(ctx->pd, kv_store->datas[i]->rdv_val, value_size,
                                                    IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
              pack_response->rdv_get.rkey = mr_create->rkey;
              pack_response->rdv_get.addr = mr_create->addr;
              pack_response->rdv_get.size = value_size;
              ctx->current_buffer = id;
              return send_data(ctx);
            }
          // EAGER PROTOCOL
          pack_response->protocol = EAGER;
          memcpy(pack_response->eager_val, kv_store->datas[i]->eager_val, sizeof(pack_response->eager_val));
          ctx->current_buffer = id;
          return send_data(ctx);
        }
    }
  pack_response->protocol = EAGER;
  memcpy(pack_response->eager_val, "", sizeof(pack_response->eager_val));
  ctx->current_buffer = id;
  return send_data(ctx);
}


/**
 * Handles incoming requests (GET or SET) based on the request type and protocol.
 * @param ctx The pingpong context.
 * @param kv_store The key-value store.
 * @param data The incoming data containing the request.
 * @param id The identifier for the data buffer.
 * @return 0 on success, 1 on error.
 */
int request_to_handle(struct pingpong_context *ctx, struct kv_store *kv_store, struct data *data, size_t id) {
  // Check if the request is a GET request
  if (data->request == GET) {
      fflush(stdout);
      printf("get Request\n");
      printf("KEY: %s\n", data->key);
      // Handle the GET request and return the result
      return server_handle_get_request(ctx, data, kv_store, id);
    }
  // Check if the protocol is RENDEZVOUS
  if (data->protocol == RENDEZVOUS) {
      // Handle RENDEZVOUS protocol for SET request
      fflush(stdout);
      printf("Set Request\n");

      printf("KEY: %s\n", data->key);
    }else {
      // Handle EAGER protocol for SET request
      fflush(stdout);
      printf("Set Request\n");
      printf("KEY: %s \tVALUE: %s\n", data->key, data->eager_val);
    }

  // Handle the SET request and return the result
  return set_request_server(ctx, data, kv_store, id);
}


/**
 * This function handles the communication loop between the server and clients.
 * @param ctx Array of pingpong_context pointers representing client contexts.
 * @param kv_store Pointer to the key-value store.
 * @return Returns 0 on successful communication, 1 on failure.
 */
int chat(struct pingpong_context *ctx[NUM_CLIENT], struct kv_store *kv_store) {
  // Initializer la discussion avec chaque client de facon asynchrone en postant une receive operations
  // Post au completion queue du client pour attendre une request
  for(int client = 0 ; client < NUM_CLIENT ; client++){
      ctx[client]->size= sizeof(struct data);
      // Prepare la incoming data
      int check = pp_post_recv(ctx[client],1);
      if(check!=1){
          printf("Couldn't receive from the server");
          return 1;
        }
    }
  size_t buffer[NUM_CLIENT]={0};
  while(1){
      // passe sur chaque client (completion queue) gerant les demandes d'arrivees et leur traitement
      for (int client = 0; client < NUM_CLIENT; client++) {
          struct ibv_wc wc[WC_BATCH];
          // Sonde la completion queue du client pour verifier si il y a des request qui ont ete completes
          int ne = ibv_poll_cq(ctx[client]->cq, WC_BATCH, wc);
          if (ne < 0) { // error
              fprintf(stderr, "poll CQ failed %d\n", ne);
              return 1;
            }
          // = 0 ca veut dire quil ny a pas de work terminer
          // >=1 veut dire que de la data a ete recu par le client
          // request_to_handle va handle la demande et passer a la prochaine demande
          // Fais post_recv pour attendre la prochaine demande
          if (ne >= 1) {
              fflush(stdout);
              request_to_handle(ctx[client], kv_store, (struct data *) ctx[client]->buf[buffer[client]], buffer[client]);
              buffer[client] = (buffer[client] + 1) % MAX_REQUEST;
              ctx[client]->current_buffer = buffer[client];
              ctx[client]->size= sizeof(struct data);
              if(pp_post_recv(ctx[client],1)!=1){
                  printf("Couldn't receive from the server2\n");
                  return 1;
                }
            }
        }
    }
}

/**
 * Initialize connections to clients using the kv_open function.
 * @param clients Array of pointers to client contexts.
 */
void connect_server(void **clients) {
  for (int i = 0; i < NUM_CLIENT; i++) {
      // Initialize connection to a client and store the context in the clients array.
      kv_open(NULL, &clients[i]);
    }
}

/**
 * Initialize a new key-value store.
 * @return Pointer to the initialized kv_store structure.
 */
struct kv_store* init_kv_store() {
  // Allocate memory for the kv_store structure.
  struct kv_store* kv_store = malloc(sizeof(struct kv_store));
  kv_store->datas = malloc(sizeof(struct data*) * kv_store->capacity);

  // Allocate memory for each individual data entry and store the pointer in datas.
  for(int i = 0; i < 4; i++){
      kv_store->datas[i] = malloc(sizeof (struct data));
    }
  // Set initial capacity and size for the kv_store.
  kv_store->capacity = 4;
  return kv_store;
}

/**
 * Server-side logic for handling client connections and communication within a key-value store application.
 * @return 0 on success, or an error code on failure.
 */
int server() {
  // Initialize the key-value store.
  struct kv_store *kv_store = init_kv_store();

  // Create an array to store client contexts.
  void *clients[NUM_CLIENT];

  // Establish connections with clients.
  connect_server(clients);

  // Initiate chat (communication) between clients and the server using the provided contexts and the key-value store.
  chat((struct pingpong_context **)clients, kv_store);

  // Free memory for each data entry in the key-value store.
  for (int i = 0; i < kv_store->capacity; ++i) {
      free(kv_store->datas[i]);
    }

  // Free memory for the array of data pointers in the key-value store.
  free(kv_store->datas);

  // Free memory for the key-value store structure.
  free(kv_store);

  return 0;
}

//------------------------------------------------------------
// -------------------- TESTS -----------------------------
//------------------------------------------------------------

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


//------------------------------------------------------------
// -------------------- CALCULS THROUGHPUT -----------------------------
//------------------------------------------------------------

/**
 * Measure the throughput of SET and GET operations in the key-value store for different message sizes.
 * @param client Pointer to the client context.
 */
void run_throughput(void *client) {
  const int NUM_WARM_UP = 50;
  const int NUM_RUNS = 100;
  printf(RED "Run throughput for setting value\n" RESET);
  for (long int message_size = MSG_INIT_SIZE; message_size <= ALL_MESSAGES_SIZE; message_size *= 2) {
      printf("%s for %ld : ", (message_size < 4096) ? "EAGER SET" : "RDV SET", message_size);

      char key[12];
      snprintf(key, sizeof(key), "%ld", message_size);

      char *value = calloc(message_size, sizeof(char));
      memset(value, 'S', message_size - 1);

      // Warm-up rounds
      for (int i = 0; i < NUM_WARM_UP; i++) {
          kv_set(client, key, value);
        }

      // Start Timer
      clock_t start_time = clock();

      // Real rounds
      for (int i = 0; i < NUM_RUNS; i++) {
          kv_set(client, key, value);
        }

      // Calculate and print throughput
      clock_t end_time = clock();
      long double diff_time = (long double)(end_time - start_time) / CLOCKS_PER_SEC;
      long double gb_unit = 100 * message_size / pow(1024, 3);
      long double throughput = (gb_unit / diff_time) * 1000;
      printf("%Lf\t%s\n", throughput, "MBps");

      // Free allocated memory
      free(value);
    }

  printf(RED "Run throughput for getting value\n" RESET);
  for (long int message_size = MSG_INIT_SIZE; message_size <= ALL_MESSAGES_SIZE; message_size *= 2) {
      printf("%s for %ld : ", (message_size < 4096) ? "EAGER GET" : "RDV GET", message_size);

      char key[12];
      snprintf(key, sizeof(key), "%ld", message_size);

      // Warm-up rounds
      for (int i = 0; i < NUM_WARM_UP; i++) {
          char *receive_value;
          kv_get(client, key, &receive_value);
          kv_release(receive_value);
        }

      // Start Timer
      clock_t start_time = clock();

      // Real rounds
      for (int i = 0; i < NUM_RUNS; i++) {
          char *receive_value;
          kv_get(client, key, &receive_value);
          kv_release(receive_value);
        }

      // Calculate and print throughput
      clock_t end_time = clock();
      long double diff_time = (long double)(end_time - start_time) / CLOCKS_PER_SEC;
      long double gb_unit = 100 * message_size / pow(1024, 3);
      long double throughput = (gb_unit / diff_time) * 1000;
      printf("%Lf\t%s\n", throughput, "MBps");
    }
}


/**
 * Check and process the server name argument.
 * @param servername Pointer to store the server name.
 * @param argc The number of command-line arguments.
 * @param argv The array of command-line arguments.
 * @return 0 if server name is successfully processed, 1 if there's an error.
 */
int check_servername(char **servername,int argc, char **argv)
{
  // Store initial argc and argv values
  argc_init = argc;
  argv_init = argv;
  if (optind == argc - 1)
    *servername = strdup(argv[optind]);
  else if (optind < argc) {
      // SERVER
      usage(argv[0]);
      return 1;
    }
  return 0;
}


//------------------------------------------------------------
// -------------------- MAIN FUNCTION -----------------------------
//------------------------------------------------------------

/**
 * Main function of the key-value store server and client application.
 * @param argc Number of command-line arguments.
 * @param argv Array of command-line arguments.
 * @return 0 on successful execution, non-zero on failure.
 */
int main(int argc, char *argv[]){
  char *servername;
  check_servername(&servername,argc,argv);
  if (servername) //client
    {
      //check for a single client
//      void *kv_handle;
//      kv_open(servername, &kv_handle);
////        run_tests(kv_handle);
//      run_throughput(kv_handle);
//        run_tests_one_client(servername);

      // test many clients
        run_tests_multiple_clients(servername);
    }
  else{
      server();
    }
  return 0;
}



