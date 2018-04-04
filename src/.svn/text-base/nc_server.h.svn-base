/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _NC_SERVER_H_
#define _NC_SERVER_H_

#include <nc_core.h>

/*
 * server_pool is a collection of servers and their continuum. Each
 * server_pool is the owner of a single proxy connection and one or
 * more client connections. server_pool itself is owned by the current
 * context.
 *
 * Each server is the owner of one or more server connections. server
 * itself is owned by the server_pool.
 *
 *  +-------------+
 *  |             |<---------------------+
 *  |             |<------------+        |
 *  |             |     +-------+--+-----+----+--------------+
 *  |   pool 0    |+--->|          |          |              |
 *  |             |     | server 0 | server 1 | ...     ...  |
 *  |             |     |          |          |              |--+
 *  |             |     +----------+----------+--------------+  |
 *  +-------------+                                             //
 *  |             |
 *  |             |
 *  |             |
 *  |   pool 1    |
 *  |             |
 *  |             |
 *  |             |
 *  +-------------+
 *  |             |
 *  |             |
 *  .             .
 *  .    ...      .
 *  .             .
 *  |             |
 *  |             |
 *  +-------------+
 *            |
 *            |
 *            //
 */

/*
 *  add group between pool and server, a pool contains several groups, a group contains several servers.
 *  changed by Zhang Juncheng, 20131220
 *
 *  +-------------+
 *  |             |<---------------------+
 *  |             |<------------+        |
 *  |             |     +-------+--+-----+----+--------------+
 *  |   pool 0    |+--->|          |          |              |
 *  |             |     | group 0  | group  1 | ...     ...  |
 *  |             |  +--|--+       |          |              |--+
 *  |             |  |  +----------+----------+--------------+  |
 *  |             |  |     /|\  /|\
 *  +-------------+  |      |    +-------|------------|                      
 *  |             |  +->+---+---+--+-----|----+-------|------+
 *  |             |     |          |          |              |
 *  |             |     | server 0 | server 1 | ...     ...  |
 *  |   pool 1    |     |          |          |              |--+
 *  |             |     +----------+----------+--------------+  |
 *  |             |
 *  |             |
 *  |             |
 *  +-------------+
 *  |             |
 *  |             |
 *  .             .
 *  .    ...      .
 *  .             .
 *  |             |
 *  |             |
 *  +-------------+
 *            |
 *            |
 *            //
 */

typedef uint32_t (*hash_t)(const char *, size_t);

struct continuum {
    uint32_t index;  /* server index */
    uint32_t value;  /* hash value */
};

struct server {
    uint32_t           idx;           /* server index */
    /*struct server_pool *owner;*/        /* owner pool */
    struct server_group *owner;       /* owner group  juncheng 20131217 */

    struct string      pname;         /* name:port:weight (ref in conf_server) */
    struct string      name;          /* name (ref in conf_server) */
    uint16_t           port;          /* port */
    uint32_t           weight;        /* weight */
    int                family;        /* socket family */
    socklen_t          addrlen;       /* socket length */
    struct sockaddr    *addr;         /* socket address (ref in conf_server) */

    uint32_t           ns_conn_q;     /* # server connection */
    struct conn_tqh    s_conn_q;      /* server connection q */

    int64_t            next_retry;    /* next retry time in usec */
    uint32_t           failure_count; /* # consecutive failures */
    
    uint32_t           gid;           /* group id label, added by Juncheng, 20131205 */
    uint8_t            role;          /* master(m), secondary master(n) or slave(s) */
};
/** 
 * Server group is a group of servers (struct server), and groups make up a server pool.
 * 
 * added by juncheng, 20131217
 */
struct server_group {
    uint32_t           idx;                  /* group index */
    uint32_t           gid;                  /* group id from conf_group */
    struct string      name;                 /* group name (ref in conf_group) */
    struct array       server;               /* server[] */
    struct server_pool *owner;               /* owner pool */

    int64_t            next_retry;           /* next retry time in usec */
    uint32_t           failure_count;        /* # consecutive failures */
    
    int                gdist_type;           /* distribution type (gdist_type_t) */
    uint32_t           weight;               /* weight */
};

struct server_pool {
    uint32_t           idx;                  /* pool index */
    struct context     *ctx;                 /* owner context */

    struct conn        *p_conn;              /* proxy connection (listener) */
    uint32_t           nc_conn_q;            /* # client connection */
    struct conn_tqh    c_conn_q;             /* client connection q */

    /*struct array       server;*/           /* server[] */
    struct array       group;                /* server group[] juncheng 20131217 */
    /*int                write; */               /* rw state 1-write 0-read 2-all */

    uint32_t           ncontinuum;           /* # continuum points */
    /*uint32_t           nserver_continuum; */   /* # servers - live and dead on continuum (const) */
    uint32_t           ngroup_continuum;    /* # groups - live and dead on continuum (const) */
    struct continuum   *continuum;           /* continuum */
    /*uint32_t           nlive_server; */        /* # live server */
    uint32_t           nlive_group;         /* # live group */
    int64_t            next_rebuild;         /* next distribution rebuild time in usec */

    struct string      name;                 /* pool name (ref in conf_pool) */
    struct string      addrstr;              /* pool address (ref in conf_pool) */
    uint16_t           port;                 /* port */
    int                family;               /* socket family */
    socklen_t          addrlen;              /* socket length */
    struct sockaddr    *addr;                /* socket address (ref in conf_pool) */
    int                dist_type;            /* distribution type (dist_type_t) */
    int                key_hash_type;        /* key hash type (hash_type_t) */
    hash_t             key_hash;             /* key hasher */
    struct string      hash_tag;             /* key hash tag (ref in conf_pool) */
    int                timeout;              /* timeout in msec */
    int                backlog;              /* listen backlog */
    uint32_t           client_connections;   /* maximum # client connection */
    uint32_t           server_connections;   /* maximum # server connection */
    int64_t            server_retry_timeout; /* server retry timeout in usec */
    uint32_t           server_failure_limit; /* server failure limit */
    unsigned           auto_eject_hosts:1;   /* auto_eject_hosts? */
    unsigned           preconnect:1;         /* preconnect? */
    unsigned           redis:1;              /* redis? */
};

void server_ref(struct conn *conn, void *owner);
void server_unref(struct conn *conn);
int server_timeout(struct conn *conn);
bool server_active(struct conn *conn);
rstatus_t server_init(struct array *server, struct array *conf_server, struct server_group *sg);
void server_deinit(struct array *server);
struct conn *server_conn(struct server *server);
rstatus_t server_connect(struct context *ctx, struct server *server, struct conn *conn);
void server_close(struct context *ctx, struct conn *conn);
void server_connected(struct context *ctx, struct conn *conn);
void server_ok(struct context *ctx, struct conn *conn);

/* added 20131226 */
rstatus_t server_group_init(struct array *server_group, struct array *conf_group, struct server_pool *sp);
void server_group_deinit(struct array *server_group);

struct array *server_pool_conns(struct context *ctx, struct server_pool *pool, uint8_t *key, uint32_t keylen, uint8_t bwrite);
struct conn *server_pool_conn(struct context *ctx, struct server_pool *pool, uint8_t *key, uint32_t keylen, uint8_t bwrite);
rstatus_t server_pool_run(struct server_pool *pool);
rstatus_t server_pool_preconnect(struct context *ctx);
void server_pool_disconnect(struct context *ctx);
rstatus_t server_pool_init(struct array *server_pool, struct array *conf_pool, struct context *ctx);
void server_pool_deinit(struct array *server_pool);

#endif
