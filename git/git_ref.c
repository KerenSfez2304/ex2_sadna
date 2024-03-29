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

#include <infiniband/verbs.h>

int g_argc;
char **g_argv;

#define EAGER_PROTOCOL_LIMIT (1 << 12) /* 4KB limit */
#define MAX_TEST_SIZE (10 * EAGER_PROTOCOL_LIMIT)
#define TEST_LOCATION "~/www/"

enum packet_type {
    EAGER_GET_REQUEST,
    EAGER_GET_RESPONSE,
    EAGER_SET_REQUEST,
    // EAGER_SET_RESPONSE - not needed!

    RENDEZVOUS_GET_REQUEST,
    RENDEZVOUS_GET_RESPONSE,
    RENDEZVOUS_SET_REQUEST,
    RENDEZVOUS_SET_RESPONSE,

    struct
    list_of_pairs{
      char *key;
      char *value;
      struct list_of_pairs *next;
    };

    struct
    list_of_pairs*
    our_list = NULL;

/* server - putting key & value to the list OR returning value of some input key
 * client - asking to set key and value to the list OR asking for value of some key he wants
*/
    struct
    packet {
      enum packet_type type; /* What kind of packet/protocol is this */
      union {
          /* The actual packet type will determine which struct will be used: */

          /* EAGER PROTOCOL PACKETS */
          struct {
              char key_and_value[0]; /* null terminator between key and value */
          } eager_set_request;

          struct {
              unsigned value_length;
              char value[0];
          } eager_get_response;

          struct { /*get request - FOR server, when client will want to get the value of some key */
              char key[0];
          } eager_get_request;

          // struct {
          //    // EAGER_SET_RESPONSE - not needed!
          // } eager_set_response;

          /* RENDEZVOUS PROTOCOL PACKETS */
          struct {

              void *remote_address;
              uint32_t remote_key;
              char key[0];
          } rndv_get_request;

          struct {
              void *remote_address;
              uint32_t remote_key;
              unsigned value_length;
          } rndv_get_response;

          struct {
              void *remote_address;
              uint32_t remote_key;
              unsigned value_length;
              char key[0]; /*key <4kb*/
          } rndv_set_request;

          struct {
              void *remote_address;
              uint32_t remote_key; /*there we will insert the value to return*/
          } rndv_set_response;
      } pp;
    };

    struct
    kv_server_address {
      char *servername; /* In the last item of an array this is NULL */
      short port; /* This is useful for multiple servers on a host */
    };

    enum {
      PINGPONG_RECV_WRID = 1,
          PINGPONG_SEND_WRID = 2,
    };

    static int
    page_size;

    struct
    pingpong_context {
      struct ibv_context *context;
      struct ibv_comp_channel *channel;
      struct ibv_pd *pd;
      struct ibv_mr *mr;
      struct ibv_cq *cq;
      struct ibv_qp *qp;
      void *buf;
      int size;
      int rx_depth;
      int routs;
      int pending;
      struct ibv_port_attr portinfo;
    };

    struct
    pingpong_dest {
      int lid;
      int qpn;
      int psn;
      union ibv_gid gid;
    };

    enum
    ibv_mtu
    pp_mtu_to_enum(int
    mtu)
    {
      switch (mtu)
        {
          case 256:
            return IBV_MTU_256;
          case 512:
            return IBV_MTU_512;
          case 1024:
            return IBV_MTU_1024;
          case 2048:
            return IBV_MTU_2048;
          case 4096:
            return IBV_MTU_4096;
          default:
            return -1;
        }
    }

    uint16_t
    pp_get_local_lid(struct
    ibv_context *
    context, int
    port)
    {
      struct ibv_port_attr attr;

      if (ibv_query_port (context, port, &attr))
        return 0;

      return attr.lid;
    }

    int
    pp_get_port_info(struct
    ibv_context *
    context, int
    port,
    struct
    ibv_port_attr *
    attr)
    {
      return ibv_query_port (context, port, attr);
    }

    void
    wire_gid_to_gid(const char *
    wgid, union
    ibv_gid *
    gid)
    {
      char tmp[9];
      uint32_t v32;
      int i;

      for (tmp[8] = 0, i = 0; i < 4; ++i)
        {
          memcpy(tmp, wgid + i * 8, 8);
          sscanf (tmp, "%x", &v32);
          *(uint32_t *) (&gid->raw[i * 4]) = ntohl(v32);
        }
    }

    void
    gid_to_wire_gid(const union
    ibv_gid *
    gid, char
    wgid[])
    {
      int i;

      for (i = 0; i < 4; ++i)
        sprintf(&wgid[i * 8], "%08x", htonl (*(uint32_t *) (gid->raw
                                                            + i * 4)));
    }

    static int
    pp_connect_ctx(struct
    pingpong_context *
    ctx, int
    port, int
    my_psn,
    enum
    ibv_mtu
    mtu, int
    sl,
    struct
    pingpong_dest *
    dest, int
    sgid_idx)
    {
      struct ibv_qp_attr attr = {
          .qp_state        = IBV_QPS_RTR,
          .path_mtu        = mtu,
          .dest_qp_num        = dest->qpn,
          .rq_psn            = dest->psn,
          .max_dest_rd_atomic    = 1,
          .min_rnr_timer        = 12,
          .ah_attr        = {
              .is_global    = 0,
              .dlid        = dest->lid,
              .sl        = sl,
              .src_path_bits    = 0,
              .port_num    = port
          }
      };

      if (dest->gid.global.interface_id)
        {
          attr.ah_attr.is_global = 1;
          attr.ah_attr.grh.hop_limit = 1;
          attr.ah_attr.grh.dgid = dest->gid;
          attr.ah_attr.grh.sgid_index = sgid_idx;
        }
      if (ibv_modify_qp (ctx->qp, &attr,
                         IBV_QP_STATE |
                         IBV_QP_AV |
                         IBV_QP_PATH_MTU |
                         IBV_QP_DEST_QPN |
                         IBV_QP_RQ_PSN |
                         IBV_QP_MAX_DEST_RD_ATOMIC |
                         IBV_QP_MIN_RNR_TIMER))
        {
          fprintf (stderr, "Failed to modify QP to RTR\n");
          return 1;
        }

      attr.qp_state = IBV_QPS_RTS;
      attr.timeout = 14;
      attr.retry_cnt = 7;
      attr.rnr_retry = 7;
      attr.sq_psn = my_psn;
      attr.max_rd_atomic = 1;
      if (ibv_modify_qp (ctx->qp, &attr,
                         IBV_QP_STATE |
                         IBV_QP_TIMEOUT |
                         IBV_QP_RETRY_CNT |
                         IBV_QP_RNR_RETRY |
                         IBV_QP_SQ_PSN |
                         IBV_QP_MAX_QP_RD_ATOMIC))
        {
          fprintf (stderr, "Failed to modify QP to RTS\n");
          return 1;
        }

      return 0;
    }

    static struct
    pingpong_dest *
    pp_client_exch_dest(const char *
    servername, int
    port,
    const struct
    pingpong_dest *
    my_dest)
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

      if (asprintf (&service, "%d", port) < 0)
        return NULL;

      n = getaddrinfo (servername, service, &hints, &res);

      if (n < 0)
        {
          fprintf (stderr, "%s for %s:%d\n", gai_strerror (n), servername, port);
          free (service);
          return NULL;
        }

      for (t = res; t; t = t->ai_next)
        {
          sockfd = socket (t->ai_family, t->ai_socktype, t->ai_protocol);
          if (sockfd >= 0)
            {
              if (!connect (sockfd, t->ai_addr, t->ai_addrlen))
                break;
              close (sockfd);
              sockfd = -1;
            }
        }

      freeaddrinfo (res);
      free (service);

      if (sockfd < 0)
        {
          fprintf (stderr, "Couldn't connect to %s:%d\n", servername, port);
          return NULL;
        }

      gid_to_wire_gid (&my_dest->gid, gid);
      sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
      if (write (sockfd, msg, sizeof msg) != sizeof msg)
        {
          fprintf (stderr, "Couldn't send local address\n");
          goto out;
        }

      if (read (sockfd, msg, sizeof msg) != sizeof msg)
        {
          perror ("client read");
          fprintf (stderr, "Couldn't read remote address\n");
          goto out;
        }

      write (sockfd, "done", sizeof "done");

      rem_dest = malloc (sizeof *rem_dest);
      if (!rem_dest)
        goto out;

      sscanf (msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
      wire_gid_to_gid (gid, &rem_dest->gid);

      out:
      close (sockfd);
      return rem_dest;
    }

    static struct
    pingpong_dest *
    pp_server_exch_dest(struct
    pingpong_context *
    ctx,
    int
    ib_port, enum
    ibv_mtu
    mtu,
    int
    port, int
    sl,
    const struct
    pingpong_dest *
    my_dest,
    int
    sgid_idx)
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

      if (asprintf (&service, "%d", port) < 0)
        return NULL;

      n = getaddrinfo (NULL, service, &hints, &res);

      if (n < 0)
        {
          fprintf (stderr, "%s for port %d\n", gai_strerror (n), port);
          free (service);
          return NULL;
        }

      for (t = res; t; t = t->ai_next)
        {
          sockfd = socket (t->ai_family, t->ai_socktype, t->ai_protocol);
          if (sockfd >= 0)
            {
              n = 1;

              setsockopt (sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

              if (!bind (sockfd, t->ai_addr, t->ai_addrlen))
                break;
              close (sockfd);
              sockfd = -1;
            }
        }

      freeaddrinfo (res);
      free (service);

      if (sockfd < 0)
        {
          fprintf (stderr, "Couldn't listen to port %d\n", port);
          return NULL;
        }

      listen (sockfd, 1);
      connfd = accept (sockfd, NULL, 0);
      close (sockfd);
      if (connfd < 0)
        {
          fprintf (stderr, "accept() failed\n");
          return NULL;
        }

      n = read (connfd, msg, sizeof msg);
      if (n != sizeof msg)
        {
          perror ("server read");
          fprintf (stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
          goto out;
        }

      rem_dest = malloc (sizeof *rem_dest);
      if (!rem_dest)
        goto out;

      sscanf (msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
      wire_gid_to_gid (gid, &rem_dest->gid);

      if (pp_connect_ctx (ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx))
        {
          fprintf (stderr, "Couldn't connect to remote QP\n");
          free (rem_dest);
          rem_dest = NULL;
          goto out;
        }

      gid_to_wire_gid (&my_dest->gid, gid);
      sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
      if (write (connfd, msg, sizeof msg) != sizeof msg)
        {
          fprintf (stderr, "Couldn't send local address\n");
          free (rem_dest);
          rem_dest = NULL;
          goto out;
        }

      read (connfd, msg, sizeof msg);

      out:
      close (connfd);
      return rem_dest;
    }

#include <sys/param.h>

    static struct
    pingpong_context *
    pp_init_ctx(struct
    ibv_device *
    ib_dev, int
    size,
    int
    rx_depth, int
    port,
    int
    use_event, int
    is_server)
    {
      struct pingpong_context *ctx;

      ctx = calloc (1, sizeof *ctx);
      if (!ctx)
        return NULL;

      ctx->size = size;
      ctx->rx_depth = rx_depth;
      ctx->routs = rx_depth;

      ctx->buf = malloc (roundup(size, page_size));
      if (!ctx->buf)
        {
          fprintf (stderr, "Couldn't allocate work buf.\n");
          return NULL;
        }

      memset(ctx->buf, 0x7b + is_server, size);

      ctx->context = ibv_open_device (ib_dev);
      if (!ctx->context)
        {
          fprintf (stderr, "Couldn't get context for %s\n",
                   ibv_get_device_name (ib_dev));
          return NULL;
        }

      if (use_event)
        {
          ctx->channel = ibv_create_comp_channel (ctx->context);
          if (!ctx->channel)
            {
              fprintf (stderr, "Couldn't create completion channel\n");
              return NULL;
            }
        }
      else
        ctx->channel = NULL;

      ctx->pd = ibv_alloc_pd (ctx->context);
      if (!ctx->pd)
        {
          fprintf (stderr, "Couldn't allocate PD\n");
          return NULL;
        }

      ctx->mr = ibv_reg_mr (ctx->pd, ctx->buf, size,
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
      if (!ctx->mr)
        {
          fprintf (stderr, "Couldn't register MR\n");
          return NULL;
        }

      ctx->cq = ibv_create_cq (ctx->context, rx_depth + 1, NULL,
                               ctx->channel, 0);
      if (!ctx->cq)
        {
          fprintf (stderr, "Couldn't create CQ\n");
          return NULL;
        }

      {
        struct ibv_qp_init_attr attr = {
            .send_cq = ctx->cq,
            .recv_cq = ctx->cq,
            .cap     = {
                .max_send_wr  = 1,
                .max_recv_wr  = rx_depth,
                .max_send_sge = 1,
                .max_recv_sge = 1
            },
            .qp_type = IBV_QPT_RC
        };

        ctx->qp = ibv_create_qp (ctx->pd, &attr);
        if (!ctx->qp)
          {
            fprintf (stderr, "Couldn't create QP\n");
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

        if (ibv_modify_qp (ctx->qp, &attr,
                           IBV_QP_STATE |
                           IBV_QP_PKEY_INDEX |
                           IBV_QP_PORT |
                           IBV_QP_ACCESS_FLAGS))
          {
            fprintf (stderr, "Failed to modify QP to INIT\n");
            return NULL;
          }
      }

      return ctx;
    }

    int
    pp_close_ctx(struct
    pingpong_context *
    ctx)
    {
      if (ibv_destroy_qp (ctx->qp))
        {
          fprintf (stderr, "Couldn't destroy QP\n");
          return 1;
        }

      if (ibv_destroy_cq (ctx->cq))
        {
          fprintf (stderr, "Couldn't destroy CQ\n");
          return 1;
        }

      if (ibv_dereg_mr (ctx->mr))
        {
          fprintf (stderr, "Couldn't deregister MR\n");
          return 1;
        }

      if (ibv_dealloc_pd (ctx->pd))
        {
          fprintf (stderr, "Couldn't deallocate PD\n");
          return 1;
        }

      if (ctx->channel)
        {
          if (ibv_destroy_comp_channel (ctx->channel))
            {
              fprintf (stderr, "Couldn't destroy completion channel\n");
              return 1;
            }
        }

      if (ibv_close_device (ctx->context))
        {
          fprintf (stderr, "Couldn't release context\n");
          return 1;
        }

      free (ctx->buf);
      free (ctx);

      return 0;
    }

    static int
    pp_post_recv(struct
    pingpong_context *
    ctx, int
    n)
    {
      struct ibv_sge list = {
          .addr    = (uintptr_t) ctx->buf,
          .length = ctx->size,
          .lkey    = ctx->mr->lkey
      };
      struct ibv_recv_wr wr = {
          .wr_id        = PINGPONG_RECV_WRID,
          .sg_list    = &list,
          .num_sge    = 1,
      };
      struct ibv_recv_wr *bad_wr;
      int i;

      for (i = 0; i < n; ++i)
        if (ibv_post_recv (ctx->qp, &wr, &bad_wr))
          break;

      return i;
    }

    static int
    pp_post_send(struct
    pingpong_context *
    ctx, enum
    ibv_wr_opcode
    opcode, unsigned
    size, const char *
    local_ptr, void *
    remote_ptr,
    uint32_t
    remote_key)
    {
      struct ibv_sge list = {
          .addr    = (uintptr_t) (local_ptr ? local_ptr : ctx->buf),
          .length = size,
          .lkey    = ctx->mr->lkey
      };
      struct ibv_send_wr wr = {
          .wr_id        = PINGPONG_SEND_WRID,
          .sg_list    = &list,
          .num_sge    = 1,
          .opcode     = opcode,
          .send_flags = IBV_SEND_SIGNALED,
      };
      struct ibv_send_wr *bad_wr;

      if (remote_ptr)
        {
          wr.wr.rdma.remote_addr = (uintptr_t) remote_ptr;
          wr.wr.rdma.rkey = remote_key;
        }

      return ibv_post_send (ctx->qp, &wr, &bad_wr);
    }

    static void
    usage(const char *
    argv0)
    {
      printf ("Usage:\n");
      printf ("  %s            start a server and wait for connection\n", argv0);
      printf ("  %s <host>     connect to server at <host>\n", argv0);
      printf ("\n");
      printf ("Options:\n");
      printf ("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
      printf ("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
      printf ("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
      printf ("  -s, --size=<size>      size of message to exchange (default 4096)\n");
      printf ("  -m, --mtu=<size>       path MTU (default 1024)\n");
      printf ("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
      printf ("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
      printf ("  -l, --sl=<sl>          service level value\n");
      printf ("  -e, --events           sleep on CQ events (default poll)\n");
      printf ("  -g, --gid-idx=<gid index> local port gid index\n");
    }

/* cases for server*/
    void
    handle_server_packets_only(struct
    pingpong_context *
    ctx, struct
    packet *
    packet){
      unsigned response_size = 0;
      struct ibv_mr *mr_create;
      struct packet *response_with_mr;
      struct packet *response_with_val;
      struct packet *response_with_loc;

      struct list_of_pairs *curr = our_list;//pointer to owr list
      switch (packet->type)
        {
          int key_exist;
          int size_of_val;
          int key_len;
          int val_len;
          char *val_to_add;
          int num_servers;
          char *inp_key;
          struct list_of_pairs *current;
          /* Only handle packets relevant to the server here - client will handle inside get/set() calls */
          case EAGER_GET_REQUEST:
            //;
            /*server got an GET REQUEST, means client sent key, and want respone=value*/

            key_exist = -1;//boolean if key exist or not
          while (curr != NULL && key_exist == -1)
            {
              if (strcmp (curr->key, packet->pp.eager_get_request.key) == 0)
                {//if we found the actual key in our list of pairs
                  size_of_val = strlen (curr->value) + 1;
                  if (size_of_val > (EAGER_PROTOCOL_LIMIT))
                    {
                      //goto LABEL_GET_RENDEZVOUS;
                      mr_create = ibv_reg_mr (ctx->pd, curr->value, size_of_val,
                                              IBV_ACCESS_REMOTE_WRITE
                                              | IBV_ACCESS_LOCAL_WRITE
                                              | IBV_ACCESS_REMOTE_READ);
                      response_with_mr = (struct packet *) ctx->buf;//we create a respone packet
                      response_with_mr->type = RENDEZVOUS_GET_RESPONSE;
                      response_with_mr->pp.rndv_get_response.remote_address = mr_create->addr;
                      response_with_mr->pp.rndv_get_response.remote_key = mr_create->rkey;
                      response_with_mr->pp.rndv_get_response.value_length = size_of_val;
                      response_size = sizeof (struct packet); ////can change to
                      key_exist = 1;
                      break;
                    }
                  response_with_val = (struct packet *) ctx->buf;//we create a respone packet
                  response_with_val->type = EAGER_GET_RESPONSE; //make the packet a type of get respone for the client
                  strcpy(response_with_val->pp.eager_get_response.value, curr->value); //putting there the value that the client should get
                  response_with_val->pp.eager_get_response.value_length =
                      strlen (curr->value)
                      + 1; //the length of value will be +1, null
                  response_size = (unsigned) (
                      (void *) packet->pp.eager_get_response.value
                      - (void *) packet + strlen (curr->value) + 1);
                  key_exist = 1;
                }
              else
                {
                  curr = curr->next;
                }
            }
          if (key_exist == -1)
            { //ELSE IF KEY NOT EXIST, we will sent to the client null
              response_with_val = (struct packet *) ctx->buf;//we create a respone packet
              response_with_val->type = EAGER_GET_RESPONSE; //make the packet a type of get respone for the client
              response_with_val->pp.eager_get_response.value[0] = 0;//the value will be NULL
              response_with_val->pp.eager_get_response.value_length = 1;//length 0+1=1
              response_size = sizeof (response_with_val)
                              + 1;//total size of packet will be the packet PLUS the value(=null=0) +1,null

            }
          break;

          case EAGER_SET_REQUEST:/*server got an SET REQUEST, means client sent keyANDvalue, and want respone=value*/
            key_exist = -1;//boolean if key exist or not
          key_len = strlen (packet->pp.eager_set_request.key_and_value)
                    + 1;//get the length of key+null terminator
          val_len =
              strlen (packet->pp.eager_set_request.key_and_value + key_len)
              + 1;//get len of value+null terminator
          val_to_add = strdup (
              packet->pp.eager_set_request.key_and_value + key_len);
          while (curr != NULL && key_exist == -1)
            {
              if (strcmp (curr->key, packet->pp.eager_set_request.key_and_value)
                  == 0)
                {//if key already exist
                  free (curr->value);
                  curr->value = (char *) malloc (val_len);
                  strcpy(curr->value, val_to_add);
                  key_exist = 1;

                }

              curr = curr->next;

            }

          if (key_exist == -1)
            {//if key not exists - creating new friend in our list
              current = (struct list_of_pairs *) malloc (sizeof (char *) * 2
                                                         + sizeof (struct list_of_pairs));//creating new node=pair
              current->key = calloc (key_len, 1);//creating key
              current->value = calloc (val_len, 1);//creating val
              strcpy(current->key, packet->pp.eager_set_request.key_and_value);
              strcpy(current->value,
                     packet->pp.eager_set_request.key_and_value + key_len);
              current->next = our_list;//adding this node to head of the our list
              our_list = current;//new node became the head of the list
            }

          break; //client doesnt excpect any respone.


          case RENDEZVOUS_GET_REQUEST:
            break;

          case RENDEZVOUS_SET_REQUEST: /*client send only key and value len*/
            key_exist = -1;
          key_len = strlen (packet->pp.rndv_set_request.key)
                    + 1;//get the length of key+null terminator
          val_len = packet->pp.rndv_set_request.value_length;

          while (curr != NULL && key_exist == -1)
            {

              if (strcmp (curr->key, packet->pp.rndv_set_request.key) == 0)
                {//if key already exist
                  /*if exist - override by free the curr value and than calloc*/
                  key_exist = 1;
                  free (curr->value);
                  curr->value = calloc (val_len, 1);
                  mr_create = ibv_reg_mr (ctx->pd, curr->value, val_len,
                                          IBV_ACCESS_REMOTE_WRITE
                                          | IBV_ACCESS_LOCAL_WRITE);
                  /*creating set response for rkey and remote_addr*/
                  response_with_mr = (struct packet *) ctx->buf;//we create a respone packet
                  response_with_mr->type = RENDEZVOUS_SET_RESPONSE;
                  response_with_mr->pp.rndv_set_response.remote_address = mr_create->addr;
                  response_with_mr->pp.rndv_set_response.remote_key = mr_create->rkey;
                  response_size = sizeof (struct packet); ////can change to
                }
              else
                {
                  curr = curr->next;
                }
            }

          if (key_exist == -1)
            {//if key not exists - creating new friend in our list
              curr = (struct list_of_pairs *) malloc (sizeof (struct list_of_pairs));//creating new node=pair

              curr->key = calloc (key_len, 1);//creating key
              curr->value = calloc (val_len, 1);//creating val
              /*can write only the key*/
              strcpy(curr->key, packet->pp.rndv_set_request.key);

              mr_create = ibv_reg_mr (ctx->pd, curr->value, val_len,
                                      IBV_ACCESS_REMOTE_WRITE
                                      | IBV_ACCESS_LOCAL_WRITE);
              /*creating set response for rkey and remote_addr*/
              response_with_mr = (struct packet *) ctx->buf;//we create a respone packet
              response_with_mr->type = RENDEZVOUS_SET_RESPONSE;
              response_with_mr->pp.rndv_set_response.remote_address = mr_create->addr;
              response_with_mr->pp.rndv_set_response.remote_key = mr_create->rkey;
              response_size = sizeof (struct packet); ////can change to

              curr->next = our_list;//adding this node to head of the our list
              our_list = curr;//new node became the head of the list

            }
          break;

          default:
            break;
        }

      if (response_size)
        {
          pp_post_send (ctx, IBV_WR_SEND, response_size, NULL, NULL, 0);
        }
    }

    int
    orig_main(struct
    kv_server_address *
    server, unsigned
    size, int
    argc, char *
    argv[], struct
    pingpong_context **
    result_ctx)
    {
      struct ibv_device **dev_list;
      struct ibv_device *ib_dev;
      struct pingpong_context *ctx;
      struct pingpong_dest my_dest;
      struct pingpong_dest *rem_dest;
      struct timeval start, end;
      char *ib_devname = NULL;
      char *servername = server->servername;
      int port = server->port;
      int ib_port = 1;
      enum ibv_mtu mtu = IBV_MTU_1024;
      int rx_depth = 1;
      int iters = 1000;
      int use_event = 0;
      int routs;
      int rcnt, scnt;
      int num_cq_events = 0;
      int sl = 0;
      int gidx = -1;
      char gid[33];

      srand48 (getpid () * time (NULL));

      while (1)
        {
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

          c = getopt_long (argc, argv, "p:d:i:s:m:r:n:l:eg:", long_options, NULL);
          if (c == -1)
            break;

          switch (c)
            {
              case 'p':
                port = strtol (optarg, NULL, 0);
              if (port < 0 || port > 65535)
                {
                  usage (argv[0]);
                  return 1;
                }
              break;

              case 'd':
                ib_devname = strdup (optarg);
              break;

              case 'i':
                ib_port = strtol (optarg, NULL, 0);
              if (ib_port < 0)
                {
                  usage (argv[0]);
                  return 1;
                }
              break;

              case 's':
                size = strtol (optarg, NULL, 0);
              break;

              case 'm':
                mtu = pp_mtu_to_enum (strtol (optarg, NULL, 0));
              if (mtu < 0)
                {
                  usage (argv[0]);
                  return 1;
                }
              break;

              case 'r':
                rx_depth = strtol (optarg, NULL, 0);
              break;

              case 'n':
                iters = strtol (optarg, NULL, 0);
              break;

              case 'l':
                sl = strtol (optarg, NULL, 0);
              break;

              case 'e':
                ++use_event;
              break;

              case 'g':
                gidx = strtol (optarg, NULL, 0);
              break;

              default:
                usage (argv[0]);
              return 1;
            }
        }

      /*if (optind == argc - 1)
          servername = strdup(argv[optind]);
      else if (optind < argc) {
          usage(argv[0]);
          return 1;
      }*/

      page_size = sysconf (_SC_PAGESIZE);

      dev_list = ibv_get_device_list (NULL);
      if (!dev_list)
        {
          perror ("Failed to get IB devices list");
          return 1;
        }

      if (!ib_devname)
        {
          ib_dev = *dev_list;
          if (!ib_dev)
            {
              fprintf (stderr, "No IB devices found\n");
              return 1;
            }
        }
      else
        {
          int i;
          for (i = 0; dev_list[i]; ++i)
            if (!strcmp (ibv_get_device_name (dev_list[i]), ib_devname))
              break;
          ib_dev = dev_list[i];
          if (!ib_dev)
            {
              fprintf (stderr, "IB device %s not found\n", ib_devname);
              return 1;
            }
        }

      ctx = pp_init_ctx (ib_dev, size, rx_depth, ib_port, use_event, !servername);
      if (!ctx)
        return 1;

      routs = pp_post_recv (ctx, ctx->rx_depth);
      if (routs < ctx->rx_depth)
        {
          fprintf (stderr, "Couldn't post receive (%d)\n", routs);
          return 1;
        }

      if (use_event)
        if (ibv_req_notify_cq (ctx->cq, 0))
          {
            fprintf (stderr, "Couldn't request CQ notification\n");
            return 1;
          }

      if (pp_get_port_info (ctx->context, ib_port, &ctx->portinfo))
        {
          fprintf (stderr, "Couldn't get port info\n");
          return 1;
        }

      my_dest.lid = ctx->portinfo.lid;
      if (ctx->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND
          && !my_dest.lid)
        {
          fprintf (stderr, "Couldn't get local LID\n");
          return 1;
        }

      if (gidx >= 0)
        {
          if (ibv_query_gid (ctx->context, ib_port, gidx, &my_dest.gid))
            {
              fprintf (stderr, "Could not get local gid for gid index %d\n", gidx);
              return 1;
            }
        }
      else
        memset(&my_dest.gid, 0, sizeof my_dest.gid);

      my_dest.qpn = ctx->qp->qp_num;
      my_dest.psn = lrand48 () & 0xffffff;
      inet_ntop (AF_INET6, &my_dest.gid, gid, sizeof gid);
      printf ("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
              my_dest.lid, my_dest.qpn, my_dest.psn, gid);

      if (servername)
        rem_dest = pp_client_exch_dest (servername, port, &my_dest);
      else
        rem_dest = pp_server_exch_dest (ctx, ib_port, mtu, port, sl, &my_dest, gidx);

      if (!rem_dest)
        return 1;

      inet_ntop (AF_INET6, &rem_dest->gid, gid, sizeof gid);
      printf ("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
              rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

      if (servername)
        if (pp_connect_ctx (ctx, ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
          return 1;

      ibv_free_device_list (dev_list);
      free (rem_dest);
      *result_ctx = ctx;
      return 0;
    }


/* | NAME OF PROGRAM | -p PORT..  */
    int
    is_server(){
      int i;
      for (i = 0; i < g_argc; i++)
        {
          if ((strcmp (g_argv[i], "-p")) == 0)
            {
              return 1;
            }
        }
      return 0;
    }


    int
    pp_wait_completions(struct
    pingpong_context *
    ctx, int
    iters)
    {
      int rcnt, scnt, num_cq_events, use_event = 0;
      rcnt = scnt = 0;
      while (rcnt + scnt < iters)
        {
          struct ibv_wc wc[2];
          int ne, i;

          do
            {
              ne = ibv_poll_cq (ctx->cq, 2, wc);
              if (ne < 0)
                {
                  fprintf (stderr, "poll CQ failed %d\n", ne);
                  return 1;
                }

            }
          while (ne < 1);

          for (i = 0; i < ne; ++i)
            {
              if (wc[i].status != IBV_WC_SUCCESS)
                {
                  fprintf (stderr, "Failed status %s (%d) for wr_id %d\n",
                           ibv_wc_status_str (wc[i].status),
                           wc[i].status, (int) wc[i].wr_id);
                  return 1;
                }

              switch ((int) wc[i].wr_id)
                {
                  case PINGPONG_SEND_WRID:
                    scnt++;
                  break;

                  case PINGPONG_RECV_WRID:
                    if (is_server () == 1)
                      {
                        handle_server_packets_only (ctx, ctx->buf);
                      }
                  pp_post_recv (ctx, 1);
                  rcnt++;
                  break;

                  default:
                    fprintf (stderr, "Completion for unknown wr_id %d\n",
                             (int) wc[i].wr_id);
                  return 1;
                }
            }
        }
      return 0;
    }

    int
    kv_open(struct
    kv_server_address *
    server, void **
    kv_handle)
    {
      return orig_main (server, EAGER_PROTOCOL_LIMIT, g_argc, g_argv, (struct pingpong_context **) kv_handle);
    }
/*client want to sent EAGER_SET_REQUEST to server - so client need to sent key and value */
    int
    kv_set(void *
    kv_handle, const char *
    key, const char *
    value)
    {
      struct pingpong_context *ctx = kv_handle;
      struct packet *set_packet = (struct packet *) ctx->buf;
      unsigned packet_size =
          strlen (key) + strlen (value) + sizeof (struct packet) + 2;

      if (packet_size < (EAGER_PROTOCOL_LIMIT))
        {
          /* Eager protocol - exercise part 1 */
          set_packet->type = EAGER_SET_REQUEST;
          strcpy(set_packet->pp.eager_set_request.key_and_value, key);
          strncpy(set_packet->pp.eager_set_request.key_and_value
                  + strlen (key), "\0", 1);
          strcpy(set_packet->pp.eager_set_request.key_and_value + strlen (key)
                 + 1, value);//null between key and value
          strncpy(
              set_packet->pp.eager_set_request.key_and_value + strlen (key) + 1
              + strlen (value), "\0", 1);
          pp_post_send (ctx, IBV_WR_SEND, packet_size, NULL, NULL, 0); /* Sends the packet to the server */
          return pp_wait_completions (ctx, 1); /* await EAGER_SET_REQUEST completion */
        }
/*-------------------------------------------------------------------------------------------------*/
      /* Otherwise, use RENDEZVOUS - exercise part 2 */
      set_packet->type = RENDEZVOUS_SET_REQUEST;
      packet_size = strlen (key) + sizeof (struct packet) + 1;
      /*client have key&value , will send him only the value length and the actual key*/
      int val_len = strlen (value) + 1;

      set_packet->pp.rndv_set_request.value_length = val_len;
      strcpy(set_packet->pp.rndv_set_request.key, key);

      pp_post_recv (ctx, 1); /* Posts a receive-buffer for RENDEZVOUS_SET_RESPONSE */
      pp_post_send (ctx, IBV_WR_SEND,
                    sizeof (struct packet) + strlen (key) + 1, NULL, NULL, 0);

      assert(
          pp_wait_completions (ctx, 2) == 0); /* wait for both to complete */
      /*return means server created to us mr for key&value*/
      assert(set_packet->type == RENDEZVOUS_SET_RESPONSE);
      /*give remote_addr and then remote_key*/
      /*server already write for me the key*/
      void *remote_addr = set_packet->pp.rndv_set_response.remote_address;
      uint32_t remote_k = set_packet->pp.rndv_set_response.remote_key;


      /*client want to write in RDMA so must also do memory registration , and that will be by ctx->mr*/
      struct ibv_mr *ctxMR = (struct ibv_mr *) ctx->mr;
      struct ibv_mr *clientMR = ibv_reg_mr (ctx->pd, (void *) value, val_len,
                                            IBV_ACCESS_REMOTE_WRITE
                                            | IBV_ACCESS_LOCAL_WRITE);
      ctx->mr = (struct ibv_mr *) clientMR;
      pp_post_send (ctx, IBV_WR_RDMA_WRITE, val_len, value, remote_addr, remote_k);
      int ret = pp_wait_completions (ctx, 1); /* wait for both to complete */
      /*give ctx->mr the real mr he had*/
      ctx->mr = (struct ibv_mr *) ctxMR;
      ibv_dereg_mr (clientMR);
      return ret;
    }

/* client want to send EAGER_GET_REQUEST to server - so client need to sent key */
    int
    kv_get(void *
    kv_handle, const char *
    key, char **
    value)
    {
      struct pingpong_context *ctx = kv_handle;
      struct packet *get_packet = (struct packet *) ctx->buf;

      unsigned packet_size =
          strlen (key) + sizeof (struct packet) + 1;//one null - of key
      if (packet_size < (EAGER_PROTOCOL_LIMIT))
        { //ALWAYS BE <4Kb because of the assumption that key always<4kb
          /* Eager protocol */
          get_packet->type = EAGER_GET_REQUEST;
          strcpy(get_packet->pp.eager_get_request.key, key);

          pp_post_recv (ctx, 1); /*get the answer on the packet*/
          pp_post_send (ctx, IBV_WR_SEND, packet_size, NULL, NULL, 0); /* Sends the packet to the server */

          pp_wait_completions (ctx, 2);
          //wait for signal that the packet got to server and for the server will
          //return the value of the key that the client just send.
          //check if packet got response from kind rndv or eager (key always<4k but value can be this or bigger==rdnv)
          struct packet *response_with_valORmr = (struct packet *) ctx->buf;
          if (response_with_valORmr->type == EAGER_GET_RESPONSE)
            {

              *value = (char *) malloc (response_with_valORmr->pp.eager_get_response.value_length);
              strcpy(*value, response_with_valORmr->pp.eager_get_response.value);
              return 0;
            }

            /*Otherwise - use RENDEZVOUS  */ //server send only the len of value and not the val itself
          else
            {
              int val_len = response_with_valORmr->pp.rndv_get_response.value_length;
              void *remote_addr = response_with_valORmr->pp.rndv_get_response.remote_address;
              uint32_t remote_k = response_with_valORmr->pp.rndv_get_response.remote_key;

              *value = calloc (val_len, 1);
              char *for_val = calloc (val_len, 1);
              struct ibv_mr *ctxMR = ctx->mr;
              struct ibv_mr *clientMR = ibv_reg_mr (ctx->pd, (void *) for_val, val_len,
                                                    IBV_ACCESS_REMOTE_WRITE
                                                    | IBV_ACCESS_LOCAL_WRITE);
              ctx->mr = (struct ibv_mr *) clientMR;
              pp_post_send (ctx, IBV_WR_RDMA_READ, val_len, for_val, remote_addr, remote_k);
              int ret = pp_wait_completions (ctx, 1); /* wait for both to complete */
              ctx->mr = (struct ibv_mr *) ctxMR;
              ibv_dereg_mr (clientMR);
              strcpy(*value, for_val);
              free (for_val);
              return ret;
            }

        }
      return 0;
    }

    void
    kv_release(char *
    value)
    {
      free (value);
    }

    int
    kv_close(void *
    kv_handle)
    {
      return pp_close_ctx ((struct pingpong_context *) kv_handle);
    }

#ifdef EX3
#define my_open  kv_open
#define set      kv_set
#define get      kv_get
#define release  kv_release
#define my_close kv_close
#endif /* EX3 */

    struct
    kv_server_address
    parser_input(char*
    ip, char*
    port){
      struct kv_server_address server = {
          .servername = NULL,
          .port = 0
      };
      char *srvname = calloc (strlen (ip) + 1, 1);
      strcpy(srvname, ip);
      server.servername = srvname;

      server.port = atoi (port);

      return server;

    }


/*                     indexer  server1   server 2 ..
 *                     1    2    3  4      5   6
/* | NAME OF PROGRAM | IP PORT  IP PORT   IP PORT   ..  */
//struct kv_server_address run_client_get_indexer()//

    struct
    kv_server_address*
    run_client_get_servers(){
      //len of server = argc - (1for-name-of-program + 2-for-indexer-inputs)
      struct kv_server_address *servers = calloc (
          g_argc - 3, sizeof (struct kv_server_address));
      int i;
      int place = 2;
      for (i = 3; i < g_argc; i = i + 2)
        {
          servers[place - 2] = parser_input (g_argv[i], g_argv[i + 1]);
          place = place + 1;
        }

      return servers;
    }



/* argv0            argv1    argv2*/
/*NAME OF PROGRAM    -P    PORT*/
    void
    run_server()
    {
      struct pingpong_context *ctx;
      struct kv_server_address server = {
          .servername = 0,
          .port = atoi (g_argv[2])

      };
      assert(0
             == orig_main (&server, EAGER_PROTOCOL_LIMIT, g_argc, g_argv, &ctx));
      while (0 <= pp_wait_completions (ctx, 1));
      pp_close_ctx (ctx);
    }

    int
    main(int
    argc, char **
    argv)
    {
      void *kv_ctx; /* handle to internal KV-client context */

      char *send_buffer = (char *) malloc (MAX_TEST_SIZE); /*malloc to buff*/
      char *recv_buffer;

      g_argc = argc;
      g_argv = argv;

      /*IF SERVER / INDEXER*/
      if (is_server () == 1)
        {
          run_server ();
        }


        /******IF CLIENT*******/
      else
        {

          if (argc < 3)
            {
              printf ("Wrong Input\n");
              return 1;
            }

          struct kv_server_address indexer[2] = {0};
          struct kv_server_address *servers;
          servers = run_client_get_servers ();

          char *srvname = calloc (strlen (argv[1]) + 1, 1);
          strcpy(srvname, argv[1]);
          indexer[0].servername = srvname;
          indexer[0].port = atoi (argv[2]);

          assert(0 == my_open (&servers[0], &kv_ctx));

          /* Test small size */
          assert(100 < MAX_TEST_SIZE);
          memset(send_buffer, 'a', 100);
          assert(0 == set (kv_ctx, "1", send_buffer));
          assert(0 == get (kv_ctx, "1", &recv_buffer));
          assert(0 == strcmp (send_buffer, recv_buffer));

          release (recv_buffer);


          /* Test logic */
          assert(0 == set (kv_ctx, "test1", send_buffer));
          assert(0 == get (kv_ctx, "test1", &recv_buffer));
          assert(0 == strcmp (send_buffer, recv_buffer));
          release (recv_buffer);
          memset(send_buffer, 'b', 100);
          assert(0 == set (kv_ctx, "1", send_buffer));
          memset(send_buffer, 'c', 100);
          assert(0 == set (kv_ctx, "22", send_buffer));
          memset(send_buffer, 'b', 100);
          assert(0 == get (kv_ctx, "1", &recv_buffer));
          assert(0 == strcmp (send_buffer, recv_buffer));
          release (recv_buffer);


          /* Test large size */
          memset(send_buffer, 'a', MAX_TEST_SIZE - 1);
          assert(0 == set (kv_ctx, "1", send_buffer));
          assert(0 == set (kv_ctx, "333", send_buffer));
          assert(0 == get (kv_ctx, "1", &recv_buffer));
          assert(0 == strcmp (send_buffer, recv_buffer));

          release (recv_buffer);

          printf ("Success\n");
          my_close (kv_ctx);
        }
      return 0;
    }