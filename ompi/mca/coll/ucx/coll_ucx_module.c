/*
 * Copyright (c) 2020      Huawei Technologies Co., Ltd. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "ompi/op/op.h"

#include "coll_ucx.h"
#include "coll_ucx_request.h"
#include "coll_ucx_datatype.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

static inline int mca_coll_ucx_get_world_rank(ompi_communicator_t *comm, int rank)
{
    ompi_proc_t *proc = ompi_comm_peer_lookup(comm, rank);

    return ((ompi_process_name_t*)&proc->super.proc_name)->vpid;
}

static inline rank_location_t mca_coll_ucx_get_rank_location(ompi_communicator_t *comm, int rank)
{
    int world_rank;

    world_rank = rank;
    if (comm != MPI_COMM_WORLD) {
        world_rank = mca_coll_ucx_get_world_rank((ompi_communicator_t *)comm, rank);
    }

    return mca_coll_ucx_component.topo.locs[world_rank];
}

static inline rank_location_t mca_coll_ucx_get_self_location(ompi_communicator_t *comm)
{
    int rank;

    rank = ompi_comm_rank(comm);
    return mca_coll_ucx_get_rank_location(comm, rank);
}

enum ucg_group_member_distance mca_coll_ucx_get_distance(void *comm, int rank1, int rank2)
{
    rank_location_t loc1;
    rank_location_t loc2;

    if (rank1 == rank2) {
        return UCG_GROUP_MEMBER_DISTANCE_SELF;
    }

    loc1 = mca_coll_ucx_get_rank_location((ompi_communicator_t *)comm, rank1);
    loc2 = mca_coll_ucx_get_rank_location((ompi_communicator_t *)comm, rank2);
    if (loc1.node_id != loc2.node_id) {
        return UCG_GROUP_MEMBER_DISTANCE_NET;
    }
    if (loc1.sock_id != loc2.sock_id) {
        return UCG_GROUP_MEMBER_DISTANCE_HOST;
    }

    return UCG_GROUP_MEMBER_DISTANCE_SOCKET;
}

static inline int mca_coll_ucx_get_node_nums(uint32_t *node_nums)
{
    int rc;
    opal_process_name_t wildcard_rank;

    wildcard_rank.jobid = ORTE_PROC_MY_NAME->jobid;
    wildcard_rank.vpid = ORTE_NAME_WILDCARD->vpid;

    /* get number of nodes in the job */
    OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, OPAL_PMIX_NUM_NODES,
                                   &wildcard_rank, &node_nums, OPAL_UINT32);

    return rc;
}

static inline int mca_coll_ucx_get_nodeid(ompi_communicator_t *comm, int rank, uint32_t *nodeid)
{
    int rc;
    ompi_proc_t *proc;

    proc = ompi_comm_peer_lookup(comm, rank);
    OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, OPAL_PMIX_NODEID,
                                   &(proc->super.proc_name), &nodeid, OPAL_UINT32);

    return rc;
}

static int mca_coll_ucx_get_sockid(uint8_t *sockid)
{
    int rc, sid;
    char *val = NULL;
    char *beg = NULL;

    OPAL_MODEX_RECV_VALUE_OPTIONAL(rc, OPAL_PMIX_LOCALITY_STRING,
                                   &(opal_proc_local_get()->proc_name), &val, OPAL_STRING);

    if (rc != OMPI_SUCCESS || val == NULL) {
        COLL_UCX_WARN("fail to get locality string, error code:%d", rc);
        return OMPI_ERROR;
    }

    /* A rank's 'loc' string example: SK%d:L3%d:L2%d:L1%d:CR%d:HT%d */
    beg = strstr(val, "SK");
    if (beg == NULL) {
        COLL_UCX_WARN("SK not exist, locality string:%s", val);
        free(val);
        return OMPI_ERROR;
    }
    beg += strlen("SK");
    sid = atoi(beg);
    *sockid = (uint8_t)sid;

    free(val);
    return OMPI_SUCCESS;
}

static int mca_coll_ucx_fill_loc_nodeid(mca_coll_ucx_module_t *module, rank_location_t *locs, int size)
{
    uint32_t nodeid, max_nodeid;
    int i, rc;

    max_nodeid = 0;
    for (i = 0; i < size; i++) {
        rc = mca_coll_ucx_get_nodeid(MPI_COMM_WORLD, i, &nodeid);
        if (rc != OMPI_SUCCESS) {
            COLL_UCX_ERROR("fail to get nodeid of rank%d, error code:%d", i, rc);
            return rc;
        }
        locs[i].node_id = nodeid;
        if (nodeid > max_nodeid) {
            max_nodeid = nodeid;
        }
    }

    mca_coll_ucx_component.topo.node_nums = max_nodeid + 1;
    COLL_UCX_VERBOSE(1, "topo.node_nums=%d", mca_coll_ucx_component.topo.node_nums);

    return OMPI_SUCCESS;
}

static int mca_coll_ucx_fill_loc_detail(mca_coll_ucx_module_t *module, rank_location_t *locs, int size)
{
    int i, rc;
    uint8_t sockid, max_sockid;
    uint8_t *sockids = NULL;

    rc = mca_coll_ucx_get_sockid(&sockid);
    if (rc != OMPI_SUCCESS) {
        /* this is not fatal error, fall back to topo aware level node */
        mca_coll_ucx_component.topo.level = COLL_UCX_TOPO_LEVEL_NODE;
        return OMPI_SUCCESS;
    }

    sockids = (uint8_t *)malloc(sizeof(uint8_t) * size);
    if (sockids == NULL) {
        COLL_UCX_ERROR("fail to alloc sockid array, rank_nums:%d", size);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    rc = ompi_coll_base_allgather_intra_bruck(&sockid, 1, MPI_UINT8_T, sockids, 1, MPI_UINT8_T,
                                              MPI_COMM_WORLD, &module->super);
    if (rc != OMPI_SUCCESS) {
        free(sockids);
        COLL_UCX_ERROR("ompi_coll_base_allgather_intra_bruck fail");
        ompi_mpi_errors_are_fatal_comm_handler(NULL, &rc, "fail to gather sockids");
        return OMPI_ERROR;
    }

    max_sockid = 0;
    for (i = 0; i < size; i++) {
        locs[i].sock_id = sockids[i];
        if (sockids[i] > max_sockid) {
            max_sockid = sockids[i];
        }
    }
    mca_coll_ucx_component.topo.sock_nums = max_sockid + 1;
    COLL_UCX_VERBOSE(1, "topo.sock_nums=%d", mca_coll_ucx_component.topo.sock_nums);

    free(sockids);
    return OMPI_SUCCESS;
}

static inline coll_ucx_topo_level_t mca_coll_ucx_get_topo_level()
{
    if (mca_coll_ucx_component.topo_aware_level >= COLL_UCX_TOPO_LEVEL_SOCKET &&
        OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy) == OPAL_BIND_TO_CORE) {
        return COLL_UCX_TOPO_LEVEL_SOCKET;
    }

    return COLL_UCX_TOPO_LEVEL_NODE;
}

static inline int mca_coll_ucx_get_topo_child_nums(coll_ucx_topo_level_t level)
{
    if (level >= COLL_UCX_TOPO_LEVEL_SOCKET) {
        return mca_coll_ucx_component.topo.sock_nums;
    }

    return mca_coll_ucx_component.topo.node_nums;
}

static inline coll_ucx_topo_tree_t *mca_coll_ucx_get_topo_child(coll_ucx_topo_tree_t *root,
                                                                coll_ucx_topo_level_t level,
                                                                int rank)
{
    int nodeid, sockid;

    if (level >= COLL_UCX_TOPO_LEVEL_SOCKET) {
        sockid = mca_coll_ucx_component.topo.locs[rank].sock_id;
        return &root->inter.child[sockid];
    }

    nodeid = mca_coll_ucx_component.topo.locs[rank].node_id;
    return &root->inter.child[nodeid];
}

static int mca_coll_ucx_build_topo_tree(coll_ucx_topo_tree_t *root,
                                        coll_ucx_topo_level_t level)
{
    int i, rc, child_nums;
    coll_ucx_topo_tree_t *child = NULL;

    if (level >= mca_coll_ucx_component.topo.level) {
        root->leaf.rank_nums = 0;
        return OMPI_SUCCESS;
    }

    level++;

    root->inter.rank_nums = 0;
    root->inter.child = NULL;
    child_nums = mca_coll_ucx_get_topo_child_nums(level);
    child = (coll_ucx_topo_tree_t *)malloc(sizeof(*child) * child_nums);
    if (child == NULL) {
        COLL_UCX_ERROR("fail to alloc children, child_nums:%d, child_level:%d, component_level:%d",
                       child_nums, level, mca_coll_ucx_component.topo.level);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    root->inter.child_nums = child_nums;
    root->inter.child = child;
    for (i = 0; i < child_nums; i++) {
        rc = mca_coll_ucx_build_topo_tree(&child[i], level);
        if (rc != OMPI_SUCCESS) {
            return rc;
        }
    }

    return OMPI_SUCCESS;
}

static void mca_coll_ucx_destroy_topo_tree(coll_ucx_topo_tree_t *root,
                                           coll_ucx_topo_level_t level)
{
    int i, child_nums;
    coll_ucx_topo_tree_t *child = NULL;

    if (level >= mca_coll_ucx_component.topo.level) {
        return;
    }

    level++;

    child = root->inter.child;
    if (child == NULL) {
        return;
    }

    child_nums = root->inter.child_nums;
    for (i = 0; i < child_nums; i++) {
        mca_coll_ucx_destroy_topo_tree(&child[i], level);
    }

    free(child);
    root->inter.child = NULL;
}

static void mca_coll_ucx_update_topo_tree(coll_ucx_topo_tree_t *root,
                                          coll_ucx_topo_level_t level,
                                          int world_rank,
                                          int comm_rank)
{
    int i, rc, child_nums;
    coll_ucx_topo_tree_t *child = NULL;

    if (level >= mca_coll_ucx_component.topo.level) {
        if (root->leaf.rank_nums == 0) {
            root->leaf.rank_min = comm_rank;
        }
        root->leaf.rank_max = comm_rank;
        root->leaf.rank_nums++;
        return;
    }

    root->inter.rank_nums++;
    level++;

    child = mca_coll_ucx_get_topo_child(root, level, world_rank);
    return mca_coll_ucx_update_topo_tree(child, level, world_rank, comm_rank);
}

static int mca_coll_ucx_init_global_topo(mca_coll_ucx_module_t *module)
{
    int i, rc, rank_nums;
    rank_location_t *locs = NULL;
    coll_ucx_topo_tree_t *root = NULL;

    if (mca_coll_ucx_component.topo.locs != NULL) {
        return OMPI_SUCCESS;
    }

    rank_nums = ompi_comm_size(MPI_COMM_WORLD);
    locs = (rank_location_t *)malloc(sizeof(*locs) * rank_nums);
    if (locs == NULL) {
        COLL_UCX_ERROR("fail to alloc rank location array, rank_nums:%d", rank_nums);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    memset(locs, 0, sizeof(*locs) * rank_nums);
    mca_coll_ucx_component.topo.locs = locs;
    mca_coll_ucx_component.topo.rank_nums = rank_nums;
    mca_coll_ucx_component.topo.level = mca_coll_ucx_get_topo_level();

    rc = mca_coll_ucx_fill_loc_nodeid(module, locs, rank_nums);
    if (rc != OMPI_SUCCESS) {
        return rc;
    }

    if (mca_coll_ucx_component.topo.level > COLL_UCX_TOPO_LEVEL_NODE) {
        rc = mca_coll_ucx_fill_loc_detail(module, locs, rank_nums);
        if (rc != OMPI_SUCCESS) {
            return rc;
        }
    }

    root = &mca_coll_ucx_component.topo.tree;
    rc = mca_coll_ucx_build_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT);
    if (rc != OMPI_SUCCESS) {
        COLL_UCX_ERROR("fail to init global topo tree");
        return rc;
    }

    for (i = 0; i < rank_nums; i++) {
        mca_coll_ucx_update_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT, i, i);
    }

    return OMPI_SUCCESS;
}

void mca_coll_ucx_destroy_global_topo()
{
    coll_ucx_topo_tree_t *root;

    root = &mca_coll_ucx_component.topo.tree;
    mca_coll_ucx_destroy_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT);

    if (mca_coll_ucx_component.topo.locs != NULL) {
        free(mca_coll_ucx_component.topo.locs);
        mca_coll_ucx_component.topo.locs = NULL;
    }
}

static int mca_coll_ucx_init_comm_topo(mca_coll_ucx_module_t *module, ompi_communicator_t *comm)
{
    int i, rc, rank_nums, global_rank;
    coll_ucx_topo_tree_t *root = NULL;

    if (comm == MPI_COMM_WORLD) {
        module->topo_tree = &mca_coll_ucx_component.topo.tree;
        return OMPI_SUCCESS;
    }

    root = (coll_ucx_topo_tree_t *)malloc(sizeof(*root));
    if (root == NULL) {
        COLL_UCX_ERROR("fail to alloc communicator topo tree root");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    module->topo_tree = root;
    rc = mca_coll_ucx_build_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT);
    if (rc != OMPI_SUCCESS) {
        COLL_UCX_ERROR("fail to init communicator topo tree");
        return rc;
    }

    rank_nums = ompi_comm_size(comm);
    for (i = 0; i < rank_nums; i++) {
        global_rank = mca_coll_ucx_get_world_rank(comm, i);
        mca_coll_ucx_update_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT, global_rank, i);
    }

    return OMPI_SUCCESS;
}

static void mca_coll_ucx_destroy_comm_topo(mca_coll_ucx_module_t *module)
{
    coll_ucx_topo_tree_t *root = module->topo_tree;

    if (root == NULL || root == &mca_coll_ucx_component.topo.tree) {
        return;
    }

    mca_coll_ucx_destroy_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT);
    free(root);
    module->topo_tree = NULL;
}

static int mca_coll_ucx_init_topo_info(mca_coll_ucx_module_t *module, ompi_communicator_t *comm)
{
    int rc;

    if (comm == MPI_COMM_WORLD) {
        rc = mca_coll_ucx_init_global_topo(module);
        if (rc != OMPI_SUCCESS) {
            return rc;
        }
    }

    return mca_coll_ucx_init_comm_topo(module, comm);
}

static inline int mca_coll_ucx_max(int a, int b)
{
    return (a > b) ? a : b;
}

static inline int mca_coll_ucx_min(int a, int b)
{
    return (a < b) ? a : b;
}

static inline void mca_coll_ucx_init_node_aware_topo_args(ucg_topo_args_t *arg)
{
    arg->ppn_max = 0;
    arg->node_nums = 0;
    arg->ppn_unbalance = 0;
    arg->pps_unbalance = 1;
    arg->nrank_uncontinue = 0;
    arg->srank_uncontinue = 1;
}

static void mca_coll_ucx_check_ppn_unbalance(ucg_topo_args_t *arg, int *pre, int cur)
{
    if (*pre == 0) {
        *pre = cur;
        return;
    }
    if (cur != *pre) {
        arg->ppn_unbalance = 1;
    }
}

static void mca_coll_ucx_check_pps_unbalance(ucg_topo_args_t *arg, int *pre, int cur)
{
    if (*pre == 0) {
        *pre = cur;
        return;
    }
    if (cur != *pre) {
        arg->pps_unbalance = 1;
    }
}

static void mca_coll_ucx_check_node_aware_tree(coll_ucx_topo_tree_t *root, ucg_topo_args_t *arg)
{
    int i, prev;
    coll_ucx_topo_tree_t *node = NULL;

    mca_coll_ucx_init_node_aware_topo_args(arg);
    for (i = 0; i < root->inter.child_nums; i++) {
        node = &root->inter.child[i];
        if (node->leaf.rank_nums == 0) {
            continue;
        }
        if (arg->node_nums == 0) {
            prev = node->leaf.rank_nums;
        }
        if (node->leaf.rank_nums != prev) {
            arg->ppn_unbalance = 1;
        }
        arg->node_nums++;
        arg->ppn_max = mca_coll_ucx_max(node->leaf.rank_nums, arg->ppn_max);

        COLL_UCX_VERBOSE(1, "node%d:rank_nums=%d,min_rank=%d,max_rank=%d",
                         i, node->leaf.rank_nums, node->leaf.rank_min, node->leaf.rank_max);

        if (node->leaf.rank_max - node->leaf.rank_min + 1 != node->leaf.rank_nums) {
            arg->nrank_uncontinue = 1;
        }
    }
}

static inline void mca_coll_ucx_init_sock_aware_topo_args(ucg_topo_args_t *arg)
{
    arg->ppn_max = 0;
    arg->node_nums = 0;
    arg->ppn_unbalance = 0;
    arg->pps_unbalance = 0;
    arg->nrank_uncontinue = 0;
    arg->srank_uncontinue = 0;
}

static inline void mca_coll_ucx_check_socket_unbalance(ucg_topo_args_t *arg, int sock_nums,
                                                       int rank_nums1, int rank_nums2)
{
#define BALANCED_SOCKET_NUM 2
    if (sock_nums > BALANCED_SOCKET_NUM || (sock_nums == BALANCED_SOCKET_NUM && rank_nums1 != rank_nums2)) {
        arg->pps_unbalance = 1;
    }
#undef BALANCED_SOCKET_NUM
}

static void mca_coll_ucx_check_sock_aware_tree(coll_ucx_topo_tree_t *root, ucg_topo_args_t *arg)
{
    int i, j, sock_nums, min, max, rank_nums1, rank_nums2;
    coll_ucx_topo_tree_t *node = NULL;
    coll_ucx_topo_tree_t *sock = NULL;
    int prev_nrank_nums = 0;
    int prev_srank_nums = 0;

    mca_coll_ucx_init_sock_aware_topo_args(arg);
    for (i = 0; i < root->inter.child_nums; i++) {
        node = &root->inter.child[i];
        if (node->inter.rank_nums == 0) {
            continue;
        }
        mca_coll_ucx_check_ppn_unbalance(arg, &prev_nrank_nums, node->inter.rank_nums);

        arg->node_nums++;
        arg->ppn_max = mca_coll_ucx_max(node->inter.rank_nums, arg->ppn_max);
        sock_nums = 0;
        rank_nums1 = 0;
        rank_nums2 = 0;
        for (j = 0; j < node->inter.child_nums; j++) {
            sock = &node->inter.child[j];
            if (sock->leaf.rank_nums == 0) {
                continue;
            }
            mca_coll_ucx_check_pps_unbalance(arg, &prev_srank_nums, sock->leaf.rank_nums);
            if (sock_nums == 0) {
                min = sock->leaf.rank_min;
                max = sock->leaf.rank_max;
                rank_nums1 = sock->leaf.rank_nums;
            } else {
                min = mca_coll_ucx_min(sock->leaf.rank_min, min);
                max = mca_coll_ucx_max(sock->leaf.rank_max, max);
                rank_nums2 = sock->leaf.rank_nums;
            }
            sock_nums++;
            if (sock->leaf.rank_max - sock->leaf.rank_min + 1 != sock->leaf.rank_nums) {
                arg->srank_uncontinue = 1;
            }
        }

        COLL_UCX_VERBOSE(1, "node%d:rank_nums=%d,min_rank=%d,max_rank=%d,sock_num=%d,sock1_nums=%d,sock2_nums=%d",
                         i, node->inter.rank_nums, min, max, sock_nums, rank_nums1, rank_nums2);

        if (max - min + 1 != node->inter.rank_nums) {
            arg->nrank_uncontinue = 1;
        }
        mca_coll_ucx_check_socket_unbalance(arg, sock_nums, rank_nums1, rank_nums2);
    }
}

static int mca_coll_ucx_get_sock_aware_pps(coll_ucx_topo_tree_t *root, rank_location_t loc)
{
    int nodeid, sockid;
    coll_ucx_topo_tree_t *node;
    coll_ucx_topo_tree_t *sock;

    nodeid = loc.node_id;
    sockid = loc.sock_id;
    node = &root->inter.child[nodeid];
    sock = &node->inter.child[sockid];
    return sock->leaf.rank_nums;
}

static int mca_coll_ucx_get_sock_aware_ppn(coll_ucx_topo_tree_t *root, rank_location_t loc)
{
    int nodeid;
    coll_ucx_topo_tree_t *node;

    nodeid = loc.node_id;
    node = &root->inter.child[nodeid];
    return node->inter.rank_nums;
}

static int mca_coll_ucx_get_node_aware_ppn(coll_ucx_topo_tree_t *root, rank_location_t loc)
{
    int nodeid;
    coll_ucx_topo_tree_t *node;

    nodeid = loc.node_id;
    node = &root->inter.child[nodeid];
    return node->leaf.rank_nums;
}

static void mca_coll_ucx_print_ucg_topo_args(const ucg_topo_args_t *arg)
{
    COLL_UCX_VERBOSE(1, "ucg_topo_args:ppn_local=%d", arg->ppn_local);
    COLL_UCX_VERBOSE(1, "ucg_topo_args:pps_local=%d", arg->pps_local);
    COLL_UCX_VERBOSE(1, "ucg_topo_args:ppn_max=%d", arg->ppn_max);
    COLL_UCX_VERBOSE(1, "ucg_topo_args:node_nums=%d", arg->node_nums);
    COLL_UCX_VERBOSE(1, "ucg_topo_args:ppn_unbalance=%d", arg->ppn_unbalance);
    COLL_UCX_VERBOSE(1, "ucg_topo_args:pps_unbalance=%d", arg->pps_unbalance);
    COLL_UCX_VERBOSE(1, "ucg_topo_args:nrank_uncontinue=%d", arg->nrank_uncontinue);
    COLL_UCX_VERBOSE(1, "ucg_topo_args:srank_uncontinue=%d", arg->srank_uncontinue);
    COLL_UCX_VERBOSE(1, "ucg_topo_args:bind_to_none=%d", arg->bind_to_none);
}

static void mca_coll_ucx_set_ucg_topo_args(mca_coll_ucx_module_t *module,
                                           ompi_communicator_t *comm,
                                           ucg_topo_args_t *arg)
{
    rank_location_t selfloc = mca_coll_ucx_get_self_location(comm);

    if (mca_coll_ucx_component.topo.level >= COLL_UCX_TOPO_LEVEL_SOCKET) {
        arg->ppn_local = mca_coll_ucx_get_sock_aware_ppn(module->topo_tree, selfloc);
        arg->pps_local = mca_coll_ucx_get_sock_aware_pps(module->topo_tree, selfloc);
        mca_coll_ucx_check_sock_aware_tree(module->topo_tree, arg);
    } else {
        arg->ppn_local = mca_coll_ucx_get_node_aware_ppn(module->topo_tree, selfloc);
        arg->pps_local = 0;
        mca_coll_ucx_check_node_aware_tree(module->topo_tree, arg);
    }

    arg->bind_to_none = (OPAL_BIND_TO_NONE == OPAL_GET_BINDING_POLICY(opal_hwloc_binding_policy));
    mca_coll_ucx_print_ucg_topo_args(arg);
}

#if OPAL_ENABLE_DEBUG
static void mca_coll_ucx_print_topo_tree(coll_ucx_topo_tree_t *root,
                                         coll_ucx_topo_level_t level)
{
    int i, child_nums;
    coll_ucx_topo_tree_t *child = NULL;

    if (level >= mca_coll_ucx_component.topo.level) {
        COLL_UCX_VERBOSE(1, "ranks info:nums=%d,min=%d,max=%d",
                         root->leaf.rank_nums,
                         root->leaf.rank_min,
                         root->leaf.rank_max);
        return;
    }

    level++;

    child = root->inter.child;
    if (child == NULL) {
        return;
    }

    child_nums = root->inter.child_nums;
    for (i = 0; i < child_nums; i++) {
        COLL_UCX_VERBOSE(1, "%s %d/%d:rank_nums=%d", (level == COLL_UCX_TOPO_LEVEL_NODE) ?
                         "node" : "sock", i, child_nums, child[i].inter.rank_nums);
        if (child[i].inter.rank_nums == 0) {
            continue;
        }
        mca_coll_ucx_print_topo_tree(&child[i], level);
    }
}

static void mca_coll_ucx_print_global_topo()
{
    int i, j, rows, len, rank_nums;
    int cols = 32;
    char logbuf[512];
    char *buf = logbuf;
    coll_ucx_topo_tree_t *root = NULL;
    rank_location_t *locs = mca_coll_ucx_component.topo.locs;

    if (locs == NULL) {
        return;
    }

    rank_nums = mca_coll_ucx_component.topo.rank_nums;
    rows = rank_nums / cols;
    for (i = 0; i < rows; i++) {
        for (j = 0; j < cols; j++) {
            len = sprintf(buf, "(%u,%u)", locs->node_id, locs->sock_id);
            locs++;
            buf += len;
        }
        *buf = '\0';
        buf = logbuf;
        COLL_UCX_VERBOSE(1, "rank %d~%d location:%s", i * cols, (i + 1) * cols - 1, buf);
    }

    if (rank_nums % cols == 0) {
        return;
    }

    for (j = rows * cols; j < rank_nums; j++) {
        len = sprintf(buf, "(%u,%u)", locs->node_id, locs->sock_id);
        locs++;
        buf += len;
    }
    *buf = '\0';
    buf = logbuf;
    COLL_UCX_VERBOSE(1, "rank %d~%d location:%s", rows * cols, rank_nums - 1, buf);
}

static void mca_coll_ucx_print_comm_topo(mca_coll_ucx_module_t *module)
{
    coll_ucx_topo_tree_t *root = module->topo_tree;

    if (root == NULL) {
        return;
    }

    mca_coll_ucx_print_topo_tree(root, COLL_UCX_TOPO_LEVEL_ROOT);
}

static void mca_coll_ucx_print_topo_info(mca_coll_ucx_module_t *module, ompi_communicator_t *comm)
{
    if (comm == MPI_COMM_WORLD) {
        mca_coll_ucx_print_global_topo();
    }

    return mca_coll_ucx_print_comm_topo(module);
}
#else
static void mca_coll_ucx_print_topo_info(mca_coll_ucx_module_t *module, ompi_communicator_t *comm)
{
}
#endif

static inline void get_operate_param(const void *mpi_op, const void *mpi_dt, int *op, int *dt)
{
    *op = mpi_op != NULL ? ((ompi_op_t *)mpi_op)->op_type : -1;
    *dt = mpi_dt != NULL ? ((ompi_datatype_t *)mpi_dt)->id : -1;
}

static int mca_coll_ucg_obtain_node_index(unsigned member_count, struct ompi_communicator_t *comm, uint16_t *node_index)
{
    ucg_group_member_index_t rank_idx, rank2_idx;
    enum ucg_group_member_distance proc_distance;
    uint16_t same_node_flag;
    uint16_t node_idx = 0;
    uint16_t init_node_idx = (uint16_t) - 1;
    int  status, status2;
    struct in_addr ip_address, ip_address2;

    /* initialize: -1: unnumbering flag */
    for (rank_idx = 0; rank_idx < member_count; rank_idx++) {
        node_index[rank_idx] = init_node_idx;
    }

    for (rank_idx = 0; rank_idx < member_count; rank_idx++) {
        if (node_index[rank_idx] == init_node_idx) {
            for (rank2_idx = rank_idx; rank2_idx < member_count; rank2_idx++) {
                proc_distance = mca_coll_ucx_get_distance(comm, rank_idx, rank2_idx);
                if ((proc_distance <= UCG_GROUP_MEMBER_DISTANCE_HOST) && (node_index[rank2_idx] == init_node_idx)) {
                    node_index[rank2_idx] = node_idx;
                }
            }
            node_idx++;
        }
    }

    /* make sure every rank has its node_index */
    for (rank_idx = 0; rank_idx < member_count; rank_idx++) {
        /* some rank do NOT have node_index */
        if (node_index[rank_idx] == init_node_idx) {
            return OMPI_ERROR;
        }
    }
    return OMPI_SUCCESS;
}

static int mca_coll_ucg_datatype_convert(ompi_datatype_t *mpi_dt,
                                         ucp_datatype_t *ucp_dt)
{
    *ucp_dt = mca_coll_ucx_get_datatype(mpi_dt);
    return 0;
}

static ptrdiff_t coll_ucx_datatype_span(void *dt_ext, int count, ptrdiff_t *gap)
{
    struct ompi_datatype_t *dtype = (struct ompi_datatype_t *)dt_ext;
    ptrdiff_t dsize, gp= 0;

    dsize = opal_datatype_span(&dtype->super, count, &gp);
    *gap = gp;
    return dsize;
}

static ucg_group_member_index_t mca_coll_ucx_get_global_member_idx(void *cb_group_obj,
                                                                   ucg_group_member_index_t index)
{
    ompi_communicator_t* comm = (ompi_communicator_t*)cb_group_obj;
    return (ucg_group_member_index_t)mca_coll_ucx_get_world_rank(comm, (int)index);
}

static void mca_coll_ucg_init_group_param(struct ompi_communicator_t *comm, ucg_group_params_t *args)
{
    args->member_index      = ompi_comm_rank(comm);
    args->member_count      = ompi_comm_size(comm);
    args->cid               = ompi_comm_get_cid(comm);
    args->mpi_reduce_f      = ompi_op_reduce;
    args->resolve_address_f = mca_coll_ucx_resolve_address;
    args->release_address_f = mca_coll_ucx_release_address;
    args->cb_group_obj      = comm;
    args->op_is_commute_f   = ompi_op_is_commute;
    args->mpi_dt_convert    = mca_coll_ucg_datatype_convert;
    args->mpi_datatype_span = coll_ucx_datatype_span;
    args->mpi_global_idx_f  = mca_coll_ucx_get_global_member_idx;
    args->mpi_rank_distance = mca_coll_ucx_get_distance;
}

static void mca_coll_ucg_arg_free(struct ompi_communicator_t *comm, ucg_group_params_t *args)
{
    unsigned i;

    if (args->node_index != NULL) {
        free(args->node_index);
        args->node_index = NULL;
    }
}

static unsigned mca_coll_ucx_calculate_ppx(const ucg_group_params_t *group_params,
                                           enum ucg_group_member_distance domain_distance)
{
    if (domain_distance == UCG_GROUP_MEMBER_DISTANCE_SOCKET) {
        return group_params->topo_args.pps_local;
    } else {
        return group_params->topo_args.ppn_local;
    }
}

static int mca_coll_ucg_create(mca_coll_ucx_module_t *module, struct ompi_communicator_t *comm)
{
    ucs_status_t error;
    ucg_group_params_t args;
    ucg_group_member_index_t my_idx;
    int status = OMPI_SUCCESS;
    unsigned i;

#if OMPI_GROUP_SPARSE
    COLL_UCX_ERROR("Sparse process groups are not supported");
    return UCS_ERR_UNSUPPORTED;
#endif

    if (mca_coll_ucx_init_topo_info(module, comm) != OMPI_SUCCESS) {
        COLL_UCX_ERROR("fail to init topo info");
        return OMPI_ERROR;
    }
    mca_coll_ucx_print_topo_info(module, comm);
    mca_coll_ucx_set_ucg_topo_args(module, comm, &args.topo_args);

    /* Fill in group initialization parameters */
    my_idx                 = ompi_comm_rank(comm);
    mca_coll_ucg_init_group_param(comm, &args);
    args.node_index        = malloc(args.member_count * sizeof(*args.node_index));
    args.get_operate_param_f = get_operate_param;

    if (args.node_index == NULL) {
        MCA_COMMON_UCX_WARN("Failed to allocate memory for %lu local ranks", args.member_count);
        status = OMPI_ERROR;
        goto out;
    }

    /* Generate node_index for each process */
    status = mca_coll_ucg_obtain_node_index(args.member_count, comm, args.node_index);

    if (status != OMPI_SUCCESS) {
        status = OMPI_ERROR;
        goto out;
    }

    args.inc_param.world_rank = ompi_comm_rank(MPI_COMM_WORLD);
    error = ucg_group_create(mca_coll_ucx_component.ucg_worker, &args, &module->ucg_group);

    /* Examine comm_new return value */
    if (error != UCS_OK) {
        MCA_COMMON_UCX_WARN("ucg_new failed: %s", ucs_status_string(error));
        status = OMPI_ERROR;
        goto out;
    }

    ucs_list_add_tail(&mca_coll_ucx_component.group_head, &module->ucs_list);
    status = OMPI_SUCCESS;

out:
    mca_coll_ucg_arg_free(comm, &args);
    return status;
}

/*
 * Initialize module on the communicator
 */
static int mca_coll_ucx_module_enable(mca_coll_base_module_t *module,
                                      struct ompi_communicator_t *comm)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*) module;
    int rc;

    if (mca_coll_ucx_component.datatype_attr_keyval == MPI_KEYVAL_INVALID) {
        /* Create a key for adding custom attributes to datatypes */
        ompi_attribute_fn_ptr_union_t copy_fn;
        ompi_attribute_fn_ptr_union_t del_fn;
        copy_fn.attr_datatype_copy_fn  =
                        (MPI_Type_internal_copy_attr_function*)MPI_TYPE_NULL_COPY_FN;
        del_fn.attr_datatype_delete_fn = mca_coll_ucx_datatype_attr_del_fn;
        rc = ompi_attr_create_keyval(TYPE_ATTR, copy_fn, del_fn,
                                     &mca_coll_ucx_component.datatype_attr_keyval,
                                     NULL, 0, NULL);
        if (rc != OMPI_SUCCESS) {
            COLL_UCX_ERROR("Failed to create keyval for UCX datatypes: %d", rc);
            return rc;
        }

        COLL_UCX_FREELIST_INIT(&mca_coll_ucx_component.convs,
                               mca_coll_ucx_convertor_t,
                               128, -1, 128);
    }

    /* prepare the placeholder for the array of request* */
    module->base_data = OBJ_NEW(mca_coll_base_comm_t);
    if (NULL == module->base_data) {
        return OMPI_ERROR;
    }

    rc = mca_coll_ucg_create(ucx_module, comm);
    if (rc != OMPI_SUCCESS) {
        OBJ_RELEASE(module->base_data);
        return rc;
    }

    COLL_UCX_VERBOSE(1, "UCX Collectives Module initialized");
    return OMPI_SUCCESS;
}

static int mca_coll_ucx_ft_event(int state)
{
    return OMPI_SUCCESS;
}

static void mca_coll_ucx_module_construct(mca_coll_ucx_module_t *module)
{
    size_t nonzero = sizeof(module->super.super);
    memset((void*)module + nonzero, 0, sizeof(*module) - nonzero);

    module->super.coll_module_enable  = mca_coll_ucx_module_enable;
    module->super.ft_event            = mca_coll_ucx_ft_event;
    module->super.coll_allreduce      = mca_coll_ucx_allreduce;
    module->super.coll_barrier        = mca_coll_ucx_barrier;
    module->super.coll_bcast          = mca_coll_ucx_bcast;
    ucs_list_head_init(&module->ucs_list);
}

static void mca_coll_ucx_module_destruct(mca_coll_ucx_module_t *module)
{
    if (module->ucg_group) {
        ucg_group_destroy(module->ucg_group);
    }

    ucs_list_del(&module->ucs_list);

    mca_coll_ucx_destroy_comm_topo(module);
}

OBJ_CLASS_INSTANCE(mca_coll_ucx_module_t,
                   mca_coll_base_module_t,
                   mca_coll_ucx_module_construct,
                   mca_coll_ucx_module_destruct);
