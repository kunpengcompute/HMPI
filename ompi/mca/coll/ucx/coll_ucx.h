/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2020-2021      Huawei Technologies Co., Ltd.
 *                              All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COLL_UCX_H
#define MCA_COLL_UCX_H

#include "ompi_config.h"

#include "mpi.h"
#include "ompi/mca/mca.h"
#include "opal/memoryhooks/memory.h"
#include "opal/mca/memory/base/base.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/request/request.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/attribute/attribute.h"

#include "orte/runtime/orte_globals.h"
#include "ompi/datatype/ompi_datatype_internal.h"
#include "opal/mca/common/ucx/common_ucx.h"

#include "ucg/api/ucg_mpi.h"
#include "ucs/datastruct/list.h"
#include "coll_ucx_freelist.h"

#ifndef UCX_VERSION
#define UCX_VERSION(major, minor) (((major)<<UCX_MAJOR_BIT) | ((minor)<<UCX_MINOR_BIT))
#endif

#define COLL_UCX_ASSERT  MCA_COMMON_UCX_ASSERT
#define COLL_UCX_ERROR   MCA_COMMON_UCX_ERROR
#define COLL_UCX_WARN    MCA_COMMON_UCX_WARN
#define COLL_UCX_VERBOSE MCA_COMMON_UCX_VERBOSE

#define COLL_UCX_MAJOR_VERSION 1
#define COLL_UCX_MINOR_VERSION 1
#define COLL_UCX_RELEASE_VERSION 0

BEGIN_C_DECLS

typedef struct coll_ucx_persistent_op mca_coll_ucx_persistent_op_t;
typedef struct coll_ucx_convertor     mca_coll_ucx_convertor_t;

typedef enum {
    COLL_UCX_TOPO_LEVEL_ROOT,
    COLL_UCX_TOPO_LEVEL_NODE,
    COLL_UCX_TOPO_LEVEL_SOCKET,
    COLL_UCX_TOPO_LEVEL_L3CACHE,
} coll_ucx_topo_level_t;

typedef struct {
    uint32_t node_id    : 16;
    uint32_t sock_id    : 8;
    uint32_t reserved   : 8;
} rank_location_t;

typedef union coll_ucx_topo_tree {
    struct {
        int rank_nums;
        int child_nums;
        union coll_ucx_topo_tree *child;
    } inter;
    struct {
        int rank_nums;
        int rank_min;
        int rank_max;
    } leaf;
} coll_ucx_topo_tree_t;

typedef struct {
    int                   rank_nums;
    int                   node_nums;
    int                   sock_nums;
    coll_ucx_topo_level_t level;
    coll_ucx_topo_tree_t  tree;
    rank_location_t      *locs;
} coll_ucx_topo_info_t;

typedef struct mca_coll_ucx_component {
    /* base MCA collectives component */
    mca_coll_base_component_t super;

    /* MCA parameters */
    int                       priority;
    int                       verbose;
    int                       num_disconnect;
    int                       topo_aware_level;

    /* UCX global objects */
    ucg_context_h             ucg_context;
    ucg_worker_h              ucg_worker;
    int                       output;
    ucs_list_link_t           group_head;
    coll_ucx_topo_info_t      topo;

    /* Requests */
    mca_coll_ucx_freelist_t   persistent_ops;
    ompi_request_t            completed_send_req;
    size_t                    request_size;

    /* Datatypes */
    int                       datatype_attr_keyval;
    ucp_datatype_t            predefined_types[OMPI_DATATYPE_MPI_MAX_PREDEFINED];

    /* Converters pool */
    mca_coll_ucx_freelist_t   convs;
} mca_coll_ucx_component_t;
OMPI_MODULE_DECLSPEC extern mca_coll_ucx_component_t mca_coll_ucx_component;

typedef struct mca_coll_ucx_module {
    mca_coll_base_module_t super;

    coll_ucx_topo_tree_t  *topo_tree;

    /* UCX per-communicator context */
    ucg_group_h            ucg_group;

    /* Progress list membership */
    ucs_list_link_t        ucs_list;

    ompi_communicator_t    *comm;

    /* Saved handlers - for fallback */
    mca_coll_base_module_reduce_fn_t previous_reduce;
    mca_coll_base_module_t *previous_reduce_module;
    mca_coll_base_module_allreduce_fn_t previous_allreduce;
    mca_coll_base_module_t *previous_allreduce_module;
    mca_coll_base_module_bcast_fn_t previous_bcast;
    mca_coll_base_module_t *previous_bcast_module;
    mca_coll_base_module_barrier_fn_t previous_barrier;
    mca_coll_base_module_t *previous_barrier_module;
    mca_coll_base_module_allgather_fn_t previous_allgather;
    mca_coll_base_module_t *previous_allgather_module;
    mca_coll_base_module_allgatherv_fn_t previous_allgatherv;
    mca_coll_base_module_t *previous_allgatherv_module;
    mca_coll_base_module_alltoall_fn_t previous_alltoall;
    mca_coll_base_module_t *previous_alltoall_module;
    mca_coll_base_module_alltoallv_fn_t previous_alltoallv;
    mca_coll_base_module_t *previous_alltoallv_module;
    mca_coll_base_module_alltoallw_fn_t previous_alltoallw;
    mca_coll_base_module_t *previous_alltoallw_module;
    mca_coll_base_module_gather_fn_t previous_gather;
    mca_coll_base_module_t *previous_gather_module;
    mca_coll_base_module_gatherv_fn_t previous_gatherv;
    mca_coll_base_module_t *previous_gatherv_module;
    mca_coll_base_module_reduce_scatter_fn_t previous_reduce_scatter;
    mca_coll_base_module_t *previous_reduce_scatter_module;
    mca_coll_base_module_ibcast_fn_t previous_ibcast;
    mca_coll_base_module_t *previous_ibcast_module;
    mca_coll_base_module_ibarrier_fn_t previous_ibarrier;
    mca_coll_base_module_t *previous_ibarrier_module;
    mca_coll_base_module_iallgather_fn_t previous_iallgather;
    mca_coll_base_module_t *previous_iallgather_module;
    mca_coll_base_module_iallgatherv_fn_t previous_iallgatherv;
    mca_coll_base_module_t *previous_iallgatherv_module;
    mca_coll_base_module_iallreduce_fn_t previous_iallreduce;
    mca_coll_base_module_t *previous_iallreduce_module;
    mca_coll_base_module_ireduce_fn_t previous_ireduce;
    mca_coll_base_module_t *previous_ireduce_module;
    mca_coll_base_module_igatherv_fn_t previous_igatherv;
    mca_coll_base_module_t *previous_igatherv_module;
    mca_coll_base_module_ialltoall_fn_t previous_ialltoall;
    mca_coll_base_module_t *previous_ialltoall_module;
    mca_coll_base_module_ialltoallv_fn_t previous_ialltoallv;
    mca_coll_base_module_t *previous_ialltoallv_module;
} mca_coll_ucx_module_t;
OBJ_CLASS_DECLARATION(mca_coll_ucx_module_t);

/*
 * Component-oriented functions for using UCX collectives.
 */
int  mca_coll_ucx_open(void);
int  mca_coll_ucx_close(void);
int  mca_coll_ucx_init(void);
void mca_coll_ucx_cleanup(void);
int  mca_coll_ucx_enable(bool enable);
int  mca_coll_ucx_progress(void);

/*
 * TESTING PURPOSES: get the worker from the module.
 */
ucg_worker_h mca_coll_ucx_get_component_worker(void);

/*
 * Start persistent collectives from an array of requests.
 */
int mca_coll_ucx_start(size_t count, ompi_request_t** requests);

/*
 * Obtain the address for a remote node.
 */
ucs_status_t mca_coll_ucx_resolve_address(void *cb_group_obj, ucg_group_member_index_t idx, ucg_address_t **addr,
                                          size_t *addr_len);

/*
 * Release an obtained address for a remote node.
 */
void mca_coll_ucx_release_address(ucg_address_t *addr);

/*
 * Release location array and comm_world's topo tree when coll_ucx component close.
 */
void mca_coll_ucx_destroy_global_topo();

/*
 * The collective operations themselves.
 */
int mca_coll_ucx_allreduce(const void *sbuf, void *rbuf, int count, struct ompi_datatype_t *dtype, struct ompi_op_t *op,
                           struct ompi_communicator_t *comm, mca_coll_base_module_t *module);

int mca_coll_ucx_iallreduce(const void *sbuf, void *rbuf, int count,
                            struct ompi_datatype_t *dtype,
                            struct ompi_op_t *op,
                            struct ompi_communicator_t *comm,
                            struct ompi_request_t **request,
                            mca_coll_base_module_t *module);

int mca_coll_ucx_allreduce_init(const void *sbuf, void *rbuf, int count,
                                struct ompi_datatype_t *dtype,
                                struct ompi_op_t *op,
                                struct ompi_communicator_t *comm,
                                struct ompi_info_t *info,
                                struct ompi_request_t **request,
                                mca_coll_base_module_t *module);

int mca_coll_ucx_bcast(void *buff, int count, struct ompi_datatype_t *datatype,
                       int root, struct ompi_communicator_t *comm,
                       mca_coll_base_module_t *module);

int mca_coll_ucx_reduce(const void *sbuf, void* rbuf, int count,
                        struct ompi_datatype_t *dtype, struct ompi_op_t *op,
                        int root, struct ompi_communicator_t *comm,
                        mca_coll_base_module_t *module);

int mca_coll_ucx_scatter(const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
                         void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
                         int root, struct ompi_communicator_t *comm,
                         mca_coll_base_module_t *module);

int mca_coll_ucx_gather(const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
                        void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
                        int root, struct ompi_communicator_t *comm,
                        mca_coll_base_module_t *module);

int mca_coll_ucx_allgather(const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
                           void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
                           struct ompi_communicator_t *comm,
                           mca_coll_base_module_t *module);

int mca_coll_ucx_alltoall(const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
                          void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
                          struct ompi_communicator_t *comm,
                          mca_coll_base_module_t *module);

int mca_coll_ucx_alltoallv(const void *sbuf, const int *scounts, const int *sdispls,
                           struct ompi_datatype_t *sdtype, void *rbuf, const int *rcounts,
                           const int *rdispls, struct ompi_datatype_t *rdtype,
                           struct ompi_communicator_t *comm, mca_coll_base_module_t *module);

int mca_coll_ucx_barrier(struct ompi_communicator_t *comm, mca_coll_base_module_t *module);

int mca_coll_ucx_ineighbor_alltoallv(void *sbuf, int *scounts, int *sdisps, struct ompi_datatype_t *sdtype,
                                     void *rbuf, int *rcounts, int *rdisps, struct ompi_datatype_t *rdtype,
                                     struct ompi_communicator_t *comm, ompi_request_t ** request,
                                     mca_coll_base_module_t *module);

END_C_DECLS

#endif /* COLL_UCX_H_ */
