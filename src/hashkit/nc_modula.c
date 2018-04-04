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

#include <stdio.h>
#include <stdlib.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_hashkit.h>

#define MODULA_CONTINUUM_ADDITION   10  /* # extra slots to build into continuum */
#define MODULA_POINTS_PER_SERVER    1

rstatus_t
modula_update(struct server_pool *pool)
{
    uint32_t ngroup;             /* # group - live and dead */
    uint32_t nlive_group;        /* # live group */
    uint32_t pointer_per_group;  /* pointers per group proportional to weight */
    uint32_t pointer_counter;     /* # pointers on continuum */
    uint32_t points_per_group;   /* points per group */
    uint32_t continuum_index;     /* continuum index */
    uint32_t continuum_addition;  /* extra space in the continuum */
    uint32_t group_index;        /* group index */
    uint32_t weight_index;        /* weight index */
    uint32_t total_weight;        /* total live group weight */
    int64_t now;                  /* current timestamp in usec */

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    ngroup = array_n(&pool->group);
    nlive_group = 0;
    total_weight = 0;
    pool->next_rebuild = 0LL;

    for (group_index = 0; group_index < ngroup; group_index++) {
        struct server_group *group = array_get(&pool->group, group_index);

        if (pool->auto_eject_hosts) {
            if (group->next_retry <= now) {
                group->next_retry = 0LL;
                nlive_group++;
            } else if (pool->next_rebuild == 0LL ||
                       group->next_retry < pool->next_rebuild) {
                pool->next_rebuild = group->next_retry;
            }
        } else {
            nlive_group++;
        }

        ASSERT(group->weight > 0);

        /* count weight only for live groups */
        if (!pool->auto_eject_hosts || group->next_retry <= now) {
            total_weight += group->weight;
        }
    }

    pool->nlive_group = nlive_group;

    if (nlive_group == 0) {
        ASSERT(pool->continuum != NULL);
        ASSERT(pool->ncontinuum != 0);

        log_debug(LOG_DEBUG, "no live groups for pool %"PRIu32" '%.*s'",
                  pool->idx, pool->name.len, pool->name.data);

        return NC_OK;
    }
    log_debug(LOG_DEBUG, "%"PRIu32" of %"PRIu32" groups are live for pool "
              "%"PRIu32" '%.*s'", nlive_group, ngroup, pool->idx,
              pool->name.len, pool->name.data);

    continuum_addition = MODULA_CONTINUUM_ADDITION;
    points_per_group = MODULA_POINTS_PER_SERVER;

    /*
     * Allocate the continuum for the pool, the first time, and every time we
     * add a new group to the pool
     */
    if (total_weight > pool->ngroup_continuum) {
        struct continuum *continuum;
        uint32_t ngroup_continuum = total_weight + MODULA_CONTINUUM_ADDITION;
        uint32_t ncontinuum = ngroup_continuum *  MODULA_POINTS_PER_SERVER;

        continuum = nc_realloc(pool->continuum, sizeof(*continuum) * ncontinuum);
        if (continuum == NULL) {
            return NC_ENOMEM;
        }

        pool->continuum = continuum;
        pool->ngroup_continuum = ngroup_continuum;
        /* pool->ncontinuum is initialized later as it could be <= ncontinuum */
    }

    /* update the continuum with the groups that are live */
    continuum_index = 0;
    pointer_counter = 0;
    for (group_index = 0; group_index < ngroup; group_index++) {
        struct server_group *group = array_get(&pool->group, group_index);

        if (pool->auto_eject_hosts && group->next_retry > now) {
            continue;
        }

        for (weight_index = 0; weight_index < group->weight; weight_index++) {
            pointer_per_group = 1;

            pool->continuum[continuum_index].index = group_index;
            pool->continuum[continuum_index++].value = 0;

            pointer_counter += pointer_per_group;
        }
    }
    pool->ncontinuum = pointer_counter;

    log_debug(LOG_VERB, "updated pool %"PRIu32" '%.*s' with %"PRIu32" of "
              "%"PRIu32" groups live in %"PRIu32" slots and %"PRIu32" "
              "active points in %"PRIu32" slots", pool->idx,
              pool->name.len, pool->name.data, nlive_group, ngroup,
              pool->ngroup_continuum, pool->ncontinuum,
              (pool->ngroup_continuum + continuum_addition) * points_per_group);

    return NC_OK;

}

uint32_t
modula_dispatch(struct continuum *continuum, uint32_t ncontinuum, uint32_t hash)
{
    struct continuum *c;

    ASSERT(continuum != NULL);
    ASSERT(ncontinuum != 0);

    c = continuum + hash % ncontinuum;

    return c->index;
}
