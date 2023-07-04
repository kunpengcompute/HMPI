/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2022-2022 Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * HEADER$
 */
#include "coll_ucg.h"
#include "coll_ucg_debug.h"
#include "coll_ucg_dt.h"
#include "coll_ucg_request.h"

#include "ompi/mca/coll/base/coll_base_functions.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/ucx/pml_ucx.h"

/* Ensure coll ucg can be dlopened if global var "ompi_pml_ucx" is not existed */
mca_pml_ucx_module_t ompi_pml_ucx __attribute__((weak));

#define MCA_COLL_UCG_SET_HANDLER(_api) \
    if (mca_coll_ucg_is_api_enable(#_api)) { \
        module->super.coll_ ## _api = mca_coll_ucg_ ## _api; \
    }

#define MCA_COLL_UCG_SET_CACHE_HANDLER(_api) \
    if (mca_coll_ucg_is_api_enable(#_api)) { \
        module->super.coll_ ## _api = mca_coll_ucg_ ## _api ## _cache; \
    }

#define MCA_COLL_UCG_SAVE_FALLBACK(_api) \
    do {\
        ucg_module->previous_ ## _api            = comm->c_coll->coll_ ## _api;\
        ucg_module->previous_ ## _api ## _module = comm->c_coll->coll_ ## _api ## _module;\
        if (!comm->c_coll->coll_ ## _api || !comm->c_coll->coll_ ## _api ## _module) {\
            return OMPI_ERROR;\
        }\
        OBJ_RETAIN(ucg_module->previous_ ## _api ## _module);\
    } while(0)

#define MCA_COLL_UCG_FREE_FALLBACK(_api) \
    if (NULL != ucg_module->previous_ ## _api ## _module) { \
        OBJ_RELEASE(ucg_module->previous_ ## _api ## _module); \
    }


static int mca_coll_ucg_progress(void)
{
    ucg_progress(mca_coll_ucg_component.ucg_context);
    return OMPI_SUCCESS;
}

static ucg_status_t mca_coll_ucg_oob_allgather(const void *sendbuf, void *recvbuf, int count, void *group)
{
    int rc;
    ompi_communicator_t *comm = (ompi_communicator_t *)group;
    rc = ompi_coll_base_allgather_intra_bruck(sendbuf, count, MPI_CHAR,
                                              recvbuf, count, MPI_CHAR,
                                              comm, NULL);
    return (rc == OMPI_SUCCESS) ? UCG_OK : UCG_ERR_NO_RESOURCE;
}

static ucg_status_t mca_coll_ucg_oob_blocking_allgather(const void *sendbuf,
                                                        void *recvbuf,
                                                        int count,
                                                        void *group)
{
    int rc, i;
    ompi_communicator_t *comm = (ompi_communicator_t *)group;
    int rank = ompi_comm_rank(comm);
    int size = ompi_comm_size(comm);

    if (rank == 0) {
        //gather all rank data to recvbuf
        for (i = 0; i < size; i++) {
            if (i == 0) {
                memcpy(recvbuf, sendbuf, count);
            } else {
                rc = MCA_PML_CALL(recv((char *)recvbuf + i * count, count, MPI_CHAR, i,
                                       MCA_COLL_BASE_TAG_ALLGATHER, comm,
                                       MPI_STATUS_IGNORE));
                if (rc != OMPI_SUCCESS) {
                    goto out;
                }
            }
        }

        //bcast recvbuf to all rank
        for (i = 1; i < size; i++) {
            rc = MCA_PML_CALL(send((char *)recvbuf, size * count, MPI_CHAR, i,
                              MCA_COLL_BASE_TAG_ALLGATHER,
                              MCA_PML_BASE_SEND_STANDARD, comm));
            if (rc != OMPI_SUCCESS) {
                goto out;
            }
        }
    } else {
        //send data to rank 0
        rc = MCA_PML_CALL(send((char *)sendbuf, count, MPI_CHAR, 0, MCA_COLL_BASE_TAG_ALLGATHER,
                          MCA_PML_BASE_SEND_STANDARD, comm));
        if (rc != OMPI_SUCCESS) {
            goto out;
        }

        //recv gather data from rank 0
        rc = MCA_PML_CALL(recv((char *)recvbuf, size * count, MPI_CHAR, 0,
                          MCA_COLL_BASE_TAG_ALLGATHER, comm,
                          MPI_STATUS_IGNORE));
        if (rc != OMPI_SUCCESS) {
            goto out;
        }
    }

out:
    return (rc == OMPI_SUCCESS) ? UCG_OK : UCG_ERR_NO_RESOURCE;
}

/**
 * @brief Parse the topology file and find the subnet ID corresponding to the rank
 *
 * Temporary solution, which does not consider the overhead of repeatedly
 * opening and traversing files. This solution will be changed later.
 */
static ucg_status_t mca_coll_ucg_get_subnet_id(ucg_rank_t myrank, char *topology,
                                               int32_t *subnet_id)
{
    if (topology == NULL) {
        UCG_DEBUG("No topology file is specified");
        return UCG_ERR_NOT_FOUND;
    }

    FILE *fp = fopen(topology, "r");
    if (fp == NULL) {
        UCG_DEBUG("Topology file %s doesn't seem to exist", topology);
        return UCG_ERR_NOT_FOUND;
    }

    ucg_status_t status = UCG_OK;
    char line[1024];
    ucg_rank_t temp_rank;
    int32_t temp_id;
    while (!feof(fp)) {
        fgets(line, sizeof(line) - 1, fp);
        int rc = sscanf(line, "rank %d subnet_id %d", &temp_rank, &temp_id);
        if (rc != 2) {
            goto err;
        } else if (temp_rank == myrank) {
            *subnet_id = temp_id;
            goto out;
        }
    }
err:
    status = UCG_ERR_INVALID_PARAM;
    UCG_DEBUG("Failed to parse the topology file. Rank %d is not found", myrank);
out:
    fclose(fp);
    return status;
}

static ucg_status_t mca_coll_ucg_get_location(ucg_rank_t rank, ucg_location_t *location)
{
    ompi_communicator_t* comm = &ompi_mpi_comm_world.comm;
    if (location == NULL || rank >= ompi_comm_size(comm)) {
        return UCG_ERR_INVALID_PARAM;
    }

    int rc;
    ompi_proc_t *proc = ompi_comm_peer_lookup(comm, rank);

    location->field_mask = 0;
    // get subnet id
    int32_t subnet_id = 0;
    ucg_status_t status;
    status = mca_coll_ucg_get_subnet_id(rank, mca_coll_ucg_component.topology,
                                        &subnet_id);
    if (status == UCG_OK) {
        location->field_mask |= UCG_LOCATION_FIELD_SUBNET_ID;
        location->subnet_id = subnet_id;
    }

    // get node id
    uint32_t node_id = 0;
    uint32_t *pnode_id = &node_id;
    OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, PMIX_NODEID,
                                   &(proc->super.proc_name), &pnode_id, PMIX_UINT32);
    if (rc != OPAL_SUCCESS) {
        goto out;
    }
    location->field_mask |= UCG_LOCATION_FIELD_NODE_ID;
    location->node_id = (int32_t)node_id;

    // get socket id
    char *locality = NULL;
    OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, PMIX_LOCALITY_STRING,
                                   &(proc->super.proc_name), &locality, PMIX_STRING);
    if (rc != OPAL_SUCCESS || locality == NULL) {
        goto out;
    }
    char *socket = strstr(locality, "SK");
    if (socket == NULL) {
        goto out_free_locality;
    }
    location->field_mask |= UCG_LOCATION_FIELD_SOCKET_ID;
    location->socket_id = atoi(socket + 2);

out_free_locality:
    free(locality);
out:
    return UCG_OK;
}

static int mca_coll_ucg_get_world_rank(void *arg, int rank)
{
    ompi_communicator_t* comm = (ompi_communicator_t*)arg;
    ompi_proc_t *proc = ompi_comm_peer_lookup(comm, rank);
    return ((ompi_process_name_t*)&proc->super.proc_name)->vpid;
}

/* mca_coll_ucg_fill_oob_group is used in ucg_init */
static void mca_coll_ucg_fill_oob_group(ucg_oob_group_t *oob_group,
                                        ompi_communicator_t *comm)
{
    oob_group->allgather = mca_coll_ucg_oob_allgather;
    oob_group->myrank = (ucg_rank_t)ompi_comm_rank(comm);
    oob_group->size = (uint32_t)ompi_comm_size(comm);
    oob_group->num_local_procs = opal_process_info.num_local_peers + 1;
    oob_group->group = (void *)comm;
    return;
}

/* mca_coll_ucg_fill_group_oob_group is used in ucg_group_create.
 * If ompi_mpi_thread_multiple is true, ompi_sync_wait_mt will be nested
 * called which will cause the program hangs. */
static void mca_coll_ucg_fill_group_oob_group(ucg_oob_group_t *oob_group,
                                              ompi_communicator_t *comm)
{
    oob_group->allgather = ompi_mpi_thread_multiple ?
                           mca_coll_ucg_oob_blocking_allgather :
                           mca_coll_ucg_oob_allgather;
    oob_group->myrank = (ucg_rank_t)ompi_comm_rank(comm);
    oob_group->size = (uint32_t)ompi_comm_size(comm);
    oob_group->group = (void *)comm;
    return;
}

static void mca_coll_ucg_fill_rank_map(ucg_rank_map_t *rank_map,
                                       ompi_communicator_t *comm)
{
    rank_map->size = (uint32_t)ompi_comm_size(comm);
    if (comm == &ompi_mpi_comm_world.comm) {
        rank_map->type = UCG_RANK_MAP_TYPE_FULL;
    } else {
        rank_map->type = UCG_RANK_MAP_TYPE_CB;
        rank_map->cb.mapping = mca_coll_ucg_get_world_rank;
        rank_map->cb.arg = (void *)comm;
    }
    return;
}

static void *mca_coll_ucg_get_ucp_ep(void *arg, void *oob_group, int rank)
{
    ompi_communicator_t *comm = (ompi_communicator_t*)oob_group;
    ompi_proc_t *proc = ompi_comm_peer_lookup(comm, rank);
    ucp_ep_h ep = proc->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_PML];
    if (OPAL_LIKELY(ep != NULL)) {
        return (void*)ep;
    }

    const int nprocs = 1;
    int ret = MCA_PML_CALL(add_procs(&proc, nprocs));
    if (ret != OMPI_SUCCESS) {
        return NULL;
    }

    return (void*)ompi_comm_peer_lookup(comm, rank)->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_PML];
}

static void *mca_coll_ucg_get_ucp_worker(void *arg)
{
    if (ompi_pml_ucx.ucp_worker == NULL) {
        UCG_ERROR("ucp worker is null");
    }
    return (void*)ompi_pml_ucx.ucp_worker;
}

static int mca_coll_ucg_init(void)
{
    mca_coll_ucg_component_t *cm = &mca_coll_ucg_component;
    ucg_status_t status;
    ucg_config_h config;

    ucg_global_params_t global_params = {
        .oob_resource.get_ucp_ep = NULL,
        .oob_resource.get_ucp_worker = NULL
    };
    if (strcmp(mca_pml_base_selected_component.pmlm_version.mca_component_name , "ucx") == 0) {
        global_params.field_mask = UCG_GLOBAL_PARAMS_FIELD_OOB_RESOURCE;
        global_params.oob_resource.get_ucp_ep = mca_coll_ucg_get_ucp_ep;
        global_params.oob_resource.get_ucp_worker = mca_coll_ucg_get_ucp_worker;
    }
    status = ucg_global_init(&global_params);
    if (status != UCG_OK) {
        UCG_ERROR("UCG global init failed: %s", ucg_status_string(status));
        return OMPI_ERROR;
    }

    status = ucg_config_read(NULL, NULL, &config);
    if (status != UCG_OK) {
        UCG_ERROR("UCG config read failed: %s", ucg_status_string(status));
        ucg_global_cleanup();
        return OMPI_ERROR;
    }

    ucg_params_t params;
    params.field_mask = UCG_PARAMS_FIELD_OOB_GROUP |
                        UCG_PARAMS_FIELD_LOCATION_CB |
                        UCG_PARAMS_FIELD_THREAD_MODE;
    mca_coll_ucg_fill_oob_group(&params.oob_group, &ompi_mpi_comm_world.comm);
    params.get_location = mca_coll_ucg_get_location;
    params.thread_mode = ompi_mpi_thread_multiple ? UCG_THREAD_MODE_MULTI : UCG_THREAD_MODE_SINGLE;
    status = ucg_init(&params, config, &cm->ucg_context);
    ucg_config_release(config);
    if (status != UCG_OK) {
        UCG_ERROR("UCG context init failed: %s", ucg_status_string(status));
        ucg_global_cleanup();
        return OMPI_ERROR;
    }
    return OMPI_SUCCESS;
}

static void mca_coll_ucg_cleanup(void)
{
    mca_coll_ucg_component_t *cm = &mca_coll_ucg_component;
    ucg_cleanup(cm->ucg_context);
    cm->ucg_context = NULL;
    ucg_global_cleanup();
    return;
}

static int mca_coll_ucg_init_once(ompi_communicator_t *comm)
{
    mca_coll_ucg_component_t *cm = &mca_coll_ucg_component;
    int rc;

    if (cm->initialized) {
        return OMPI_SUCCESS;
    }

    if (comm != &ompi_mpi_comm_world.comm) {
        UCG_ERROR("The comm on first call must be mpi_comm_world");
        return OMPI_ERROR;
    }

    rc = mca_coll_ucg_conv_pool_init();
    if (rc != OMPI_SUCCESS) {
        goto err;
    }

    rc = mca_coll_ucg_rpool_init();
    if (rc != OMPI_SUCCESS) {
        goto err_cleanup_conv_pool;
    }

    uint32_t size = (uint32_t)ompi_comm_size(comm);
    rc = mca_coll_ucg_subargs_pool_init(size);
    if (rc != OMPI_SUCCESS) {
        UCG_ERROR("Failed to init subargs mpool, %d", rc);
        goto err_cleanup_rpool;
    }

    if (ompi_mpi_thread_multiple) {
        UCG_DEBUG("rcache is non-thread-safe, disable it");
        cm->max_rcache_size = 0;
    }

    if (cm->max_rcache_size > 0) {
        UCG_DEBUG("max rcache size is %d", cm->max_rcache_size);
        rc = mca_coll_ucg_rcache_init(cm->max_rcache_size);
        if (rc != OMPI_SUCCESS) {
            goto err_cleanup_subargs_pool;
        }
    }

    if (cm->disable_coll != NULL) {
        UCG_DEBUG("Disable %s", cm->disable_coll);
        cm->blacklist = opal_argv_split(cm->disable_coll, ',');
    }

    rc = mca_coll_ucg_init();
    if (rc != OMPI_SUCCESS) {
        goto err_free_blacklist;
    }

    rc = mca_coll_ucg_type_init();
    if (rc != OMPI_SUCCESS) {
        goto err_cleanup_ucg;
    }
    /* everything is ready, register progress function. */
    opal_progress_register(mca_coll_ucg_progress);
    cm->initialized = true;
    return OMPI_SUCCESS;

err_cleanup_ucg:
    mca_coll_ucg_cleanup();
err_free_blacklist:
    if (cm->blacklist != NULL) {
        opal_argv_free(cm->blacklist);
    }
    if (cm->max_rcache_size > 0) {
        mca_coll_ucg_rcache_cleanup();
    }
err_cleanup_subargs_pool:
    mca_coll_ucg_subargs_pool_cleanup();
err_cleanup_rpool:
    mca_coll_ucg_rpool_cleanup();
err_cleanup_conv_pool:
    mca_coll_ucg_conv_pool_cleanup();
err:
    return rc;
}

void mca_coll_ucg_cleanup_once(void)
{
    mca_coll_ucg_component_t *cm = &mca_coll_ucg_component;
    if (!cm->initialized) {
        return;
    }

    opal_progress_unregister(mca_coll_ucg_progress);
    mca_coll_ucg_type_cleanup();
    mca_coll_ucg_cleanup();
    if (cm->blacklist != NULL) {
        opal_argv_free(cm->blacklist);
    }
    if (cm->max_rcache_size > 0) {
        mca_coll_ucg_rcache_cleanup();
    }
    mca_coll_ucg_rpool_cleanup();
    mca_coll_ucg_conv_pool_cleanup();
    return;
}

static int mca_coll_ucg_save_fallback(mca_coll_ucg_module_t *ucg_module,
                                      ompi_communicator_t *comm)
{
    MCA_COLL_UCG_SAVE_FALLBACK(allreduce);
    MCA_COLL_UCG_SAVE_FALLBACK(bcast);
    MCA_COLL_UCG_SAVE_FALLBACK(barrier);
    MCA_COLL_UCG_SAVE_FALLBACK(alltoallv);
    MCA_COLL_UCG_SAVE_FALLBACK(scatterv);
    MCA_COLL_UCG_SAVE_FALLBACK(gatherv);
    MCA_COLL_UCG_SAVE_FALLBACK(allgatherv);

    MCA_COLL_UCG_SAVE_FALLBACK(iallreduce);
    MCA_COLL_UCG_SAVE_FALLBACK(ibcast);
    MCA_COLL_UCG_SAVE_FALLBACK(ibarrier);
    MCA_COLL_UCG_SAVE_FALLBACK(ialltoallv);
    MCA_COLL_UCG_SAVE_FALLBACK(iscatterv);
    MCA_COLL_UCG_SAVE_FALLBACK(igatherv);
    MCA_COLL_UCG_SAVE_FALLBACK(iallgatherv);

    MCA_COLL_UCG_SAVE_FALLBACK(allreduce_init);
    MCA_COLL_UCG_SAVE_FALLBACK(bcast_init);
    MCA_COLL_UCG_SAVE_FALLBACK(barrier_init);
    MCA_COLL_UCG_SAVE_FALLBACK(alltoallv_init);
    MCA_COLL_UCG_SAVE_FALLBACK(scatterv_init);
    MCA_COLL_UCG_SAVE_FALLBACK(gatherv_init);
    MCA_COLL_UCG_SAVE_FALLBACK(allgatherv_init);

    return OMPI_SUCCESS;
}

static void mca_coll_ucg_free_fallback(mca_coll_ucg_module_t *ucg_module)
{
    MCA_COLL_UCG_FREE_FALLBACK(allreduce);
    MCA_COLL_UCG_FREE_FALLBACK(bcast);
    MCA_COLL_UCG_FREE_FALLBACK(barrier);
    MCA_COLL_UCG_FREE_FALLBACK(alltoallv);
    MCA_COLL_UCG_FREE_FALLBACK(scatterv);
    MCA_COLL_UCG_FREE_FALLBACK(gatherv);
    MCA_COLL_UCG_FREE_FALLBACK(allgatherv);

    MCA_COLL_UCG_FREE_FALLBACK(iallreduce);
    MCA_COLL_UCG_FREE_FALLBACK(ibcast);
    MCA_COLL_UCG_FREE_FALLBACK(ibarrier);
    MCA_COLL_UCG_FREE_FALLBACK(ialltoallv);
    MCA_COLL_UCG_FREE_FALLBACK(iscatterv);
    MCA_COLL_UCG_FREE_FALLBACK(igatherv);
    MCA_COLL_UCG_FREE_FALLBACK(iallgatherv);

    MCA_COLL_UCG_FREE_FALLBACK(allreduce_init);
    MCA_COLL_UCG_FREE_FALLBACK(bcast_init);
    MCA_COLL_UCG_FREE_FALLBACK(barrier_init);
    MCA_COLL_UCG_FREE_FALLBACK(alltoallv_init);
    MCA_COLL_UCG_FREE_FALLBACK(scatterv_init);
    MCA_COLL_UCG_FREE_FALLBACK(gatherv_init);
    MCA_COLL_UCG_FREE_FALLBACK(allgatherv_init);

    return;
}

static int mca_coll_ucg_create_group(mca_coll_ucg_module_t *module, ompi_communicator_t *comm)
{
    ucg_status_t rc;
    mca_coll_ucg_component_t *cm = &mca_coll_ucg_component;
    ucg_group_params_t params;

    /* Set UCG group parameter*/
    params.field_mask = UCG_GROUP_PARAMS_FIELD_ID |
                        UCG_GROUP_PARAMS_FIELD_SIZE |
                        UCG_GROUP_PARAMS_FIELD_MYRANK |
                        UCG_GROUP_PARAMS_FIELD_RANK_MAP |
                        UCG_GROUP_PARAMS_FIELD_OOB_GROUP;
    params.id = ompi_comm_get_cid(comm);
    params.size = (uint32_t)ompi_comm_size(comm);
    params.myrank = (ucg_rank_t)ompi_comm_rank(comm);
    mca_coll_ucg_fill_rank_map(&params.rank_map, comm);
    mca_coll_ucg_fill_group_oob_group(&params.oob_group, comm);

    /* Initialize UCG group*/
    rc = ucg_group_create(cm->ucg_context, &params, &module->group);
    if (rc != UCG_OK) {
        UCG_ERROR("UCG create group failed: %s", ucg_status_string(rc));
        return OMPI_ERROR;
    }

    return OMPI_SUCCESS;
}

static int mca_coll_ucg_module_enable(mca_coll_base_module_t *module,
                                      ompi_communicator_t *comm)
{
    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t *)module;
    int rc;
    /* if any fails, resources will be freed in mca_coll_ucg_module_destruct() */
    rc = mca_coll_ucg_save_fallback(ucg_module, comm);
    if (rc != OMPI_SUCCESS) {
        UCG_ERROR("Failed to save coll fallback, %d", rc);
        return rc;
    }

    rc = mca_coll_ucg_create_group(ucg_module, comm);
    if (rc != OMPI_SUCCESS) {
        UCG_ERROR("Failed to create ucg group, %d", rc);
        return rc;
    }

    UCG_DEBUG("Module initialized");
    return OMPI_SUCCESS;
}

static bool mca_coll_ucg_is_api_enable(const char *api)
{
    char **blacklist = mca_coll_ucg_component.blacklist;
    if (blacklist == NULL) {
        return true;
    }

    for (; *blacklist != NULL; ++blacklist) {
        if (!strcmp(*blacklist, api)) {
            return false;
        }
    }

    return true;
}

static void mca_coll_ucg_module_construct(mca_coll_ucg_module_t *module)
{
    memset((char*)module + sizeof(module->super), 0, sizeof(*module) - sizeof(module->super));
    module->super.coll_module_enable = mca_coll_ucg_module_enable;
    if (mca_coll_ucg_component.max_rcache_size > 0) {
        MCA_COLL_UCG_SET_CACHE_HANDLER(allreduce);
        MCA_COLL_UCG_SET_CACHE_HANDLER(barrier);
        MCA_COLL_UCG_SET_CACHE_HANDLER(bcast);
        // MCA_COLL_UCG_SET_CACHE_HANDLER(alltoallv);
        // MCA_COLL_UCG_SET_CACHE_HANDLER(scatterv);
        MCA_COLL_UCG_SET_CACHE_HANDLER(gatherv);
        MCA_COLL_UCG_SET_CACHE_HANDLER(allgatherv);

        MCA_COLL_UCG_SET_CACHE_HANDLER(iallreduce);
        MCA_COLL_UCG_SET_CACHE_HANDLER(ibarrier);
        MCA_COLL_UCG_SET_CACHE_HANDLER(ibcast);
        // MCA_COLL_UCG_SET_CACHE_HANDLER(ialltoallv);
        MCA_COLL_UCG_SET_CACHE_HANDLER(iscatterv);
        MCA_COLL_UCG_SET_CACHE_HANDLER(igatherv);
        MCA_COLL_UCG_SET_CACHE_HANDLER(iallgatherv);
    } else {
        MCA_COLL_UCG_SET_HANDLER(allreduce);
        MCA_COLL_UCG_SET_HANDLER(barrier);
        MCA_COLL_UCG_SET_HANDLER(bcast);
        // MCA_COLL_UCG_SET_HANDLER(alltoallv);
        // MCA_COLL_UCG_SET_HANDLER(scatterv);
        MCA_COLL_UCG_SET_HANDLER(gatherv);
        MCA_COLL_UCG_SET_HANDLER(allgatherv);

        MCA_COLL_UCG_SET_HANDLER(iallreduce);
        MCA_COLL_UCG_SET_HANDLER(ibarrier);
        MCA_COLL_UCG_SET_HANDLER(ibcast);
        // MCA_COLL_UCG_SET_HANDLER(ialltoallv);
        MCA_COLL_UCG_SET_HANDLER(iscatterv);
        MCA_COLL_UCG_SET_HANDLER(igatherv);
        MCA_COLL_UCG_SET_HANDLER(iallgatherv);
    }

    MCA_COLL_UCG_SET_HANDLER(allreduce_init);
    MCA_COLL_UCG_SET_HANDLER(barrier_init);
    MCA_COLL_UCG_SET_HANDLER(bcast_init);
    // MCA_COLL_UCG_SET_HANDLER(alltoallv_init);
    MCA_COLL_UCG_SET_HANDLER(scatterv_init);
    MCA_COLL_UCG_SET_HANDLER(gatherv_init);
    MCA_COLL_UCG_SET_HANDLER(allgatherv_init);
    return;
}

static void mca_coll_ucg_module_destruct(mca_coll_ucg_module_t *ucg_module)
{
    if (ucg_module->group != NULL) {
        if (mca_coll_ucg_component.max_rcache_size > 0) {
            mca_coll_ucg_rcache_del_by_comm(ucg_module->comm);
        }
        ucg_group_destroy(ucg_module->group);
        ucg_module->group = NULL;
    }

    /* kv must be freed before component close */
    if (ucg_module->comm == &ompi_mpi_comm_world.comm) {
        mca_coll_ucg_type_free_kv();
    }

    mca_coll_ucg_free_fallback(ucg_module);
    return;
}

OBJ_CLASS_INSTANCE(mca_coll_ucg_module_t,
                   mca_coll_base_module_t,
                   mca_coll_ucg_module_construct,
                   mca_coll_ucg_module_destruct);

int mca_coll_ucg_init_query(bool enable_progress_threads, bool enable_mpi_threads)
{
    return OMPI_SUCCESS;
}

mca_coll_base_module_t *mca_coll_ucg_comm_query(ompi_communicator_t *comm, int *priority)
{
    mca_coll_ucg_component_t *cm = &mca_coll_ucg_component;
    mca_coll_ucg_module_t *ucg_module;

    if ((OMPI_COMM_IS_INTER(comm)) || (ompi_comm_size(comm) < 2)) {
        return NULL;
    }

    /* mca_coll_ucg_init_once() need to use the ompi_mpi_comm_world which is valid
       until now, so put mca_coll_ucg_init_once() here. It will only be executed
       once */
    if (OMPI_SUCCESS != mca_coll_ucg_init_once(comm)) {
        return NULL;
    }

    ucg_module = OBJ_NEW(mca_coll_ucg_module_t);
    if (ucg_module == NULL) {
        return NULL;
    }
    ucg_module->comm = comm;

    *priority = cm->priority;
    return &(ucg_module->super);
}
