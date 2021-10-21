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

#include "coll_ucx.h"
#include "coll_ucx_request.h"
#include "coll_ucx_datatype.h"
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "opal/runtime/opal_progress.h"
#include "ompi/message/message.h"
#include <inttypes.h>

#define MCA_COLL_UCX_PROGRESS_LOOP(_worker)                                   \
    for (unsigned iter = 0;; (++iter % opal_common_ucx.progress_iterations) ? \
                         (void)ucp_worker_progress(_worker) : opal_progress())

#define MCA_COLL_UCX_WAIT_LOOP(_request, _worker, _msg, _completed)               \
    do {                                                                          \
       ucs_status_t status;                                                       \
       /* call UCX progress */                                                    \
       MCA_COLL_UCX_PROGRESS_LOOP(_worker) {                                      \
           status = ucg_request_check_status(_request);                           \
           if (UCS_INPROGRESS != status) {                                        \
               _completed;                                                        \
               if (OPAL_LIKELY(UCS_OK == status)) {                               \
                   return OPAL_SUCCESS;                                           \
               } else {                                                           \
                   COLL_UCX_VERBOSE(1, "%s failed: %d, %s",                       \
                                    (_msg) ? (_msg) : __func__,                   \
                                    UCS_PTR_STATUS(_request),                     \
                                    ucs_status_string(UCS_PTR_STATUS(_request))); \
                   return OPAL_ERROR;                                             \
               }                                                                  \
           }                                                                      \
        }                                                                         \
    } while (0)

static inline void mca_coll_ucx_free(void **ptr)
{
    if (*ptr != NULL) {
        free(*ptr);
        *ptr = NULL;
    }
}

static inline int mca_coll_ucx_is_datatype_supported(struct ompi_datatype_t *dtype, int count)
{
    return ompi_datatype_is_contiguous_memory_layout(dtype, count);
}

static ucs_status_t mca_coll_ucx_check_total_data_size(size_t dtype_size, int count)
{
    static const uint64_t max_size = 4294967296;
    uint64_t total_size = dtype_size * count;
    return (total_size <= max_size) ? UCS_OK : UCS_ERR_OUT_OF_RANGE;
}

int mca_coll_ucx_start(size_t count, ompi_request_t** requests)
{
    mca_coll_ucx_persistent_op_t *preq = NULL;
    ompi_request_t *tmp_req = NULL;
    size_t i;

    for (i = 0; i < count; ++i) {
        preq = (mca_coll_ucx_persistent_op_t *)requests[i];
        if ((preq == NULL) || (OMPI_REQUEST_COLL != preq->ompi.req_type)) {
            /* Skip irrelevant requests */
            continue;
        }

        COLL_UCX_ASSERT(preq->ompi.req_state != OMPI_REQUEST_INVALID);
        preq->ompi.req_state = OMPI_REQUEST_ACTIVE;
        mca_coll_ucx_request_reset(&preq->ompi);

        tmp_req = ucg_collective_start_nb(preq->coll_desc);
        if (tmp_req == NULL) {
            COLL_UCX_VERBOSE(8, "collective completed immediately, completing persistent request %p", (void*)preq);
            mca_coll_ucx_set_coll_status(&preq->ompi.req_status, UCS_OK);
            ompi_request_complete(&preq->ompi, true);
        } else if (!UCS_PTR_IS_ERR(tmp_req)) {
            if (REQUEST_COMPLETE(tmp_req)) {
                /* tmp_req is already completed */
                COLL_UCX_VERBOSE(8, "completing persistent request %p", (void*)preq);
                mca_coll_ucx_persistent_op_complete(preq, tmp_req);
            } else {
                /* tmp_req would be completed by callback and trigger completion
                 * of preq */
                COLL_UCX_VERBOSE(8, "temporary request %p will complete persistent request %p",
                                 (void*)tmp_req, (void*)preq);
                tmp_req->req_complete_cb_data = preq;
                preq->tmp_req                 = tmp_req;
            }
        } else {
            COLL_UCX_ERROR("ucx collective failed: %s", ucs_status_string(UCS_PTR_STATUS(tmp_req)));
            return OMPI_ERROR;
        }
    }

    return OMPI_SUCCESS;
}

/**
 * For each type of collectives there are 3 varieties of function calls:
 * blocking, non-blocking and persistent initialization. For example, for
 * the allreduce collective operations, those would be called:
 * - mca_coll_ucx_allreduce
 * - mca_coll_ucx_iallreduce
 * - mca_coll_ucx_iallreduce_init
 *
 * In the blocking version, request is placed on the stack, awaiting completion.
 * For non-blocking, request is allocated by UCX, awaiting completion.
 * For persistent requests, the collective starts later - only then the
 * (internal) request is created (by UCX) and placed as "tmp_req" inside
 * the persistent (external) request structure.
 */
#define COLL_UCX_TRACE(_msg, _sbuf, _rbuf, _count, _datatype, _comm, ...)        \
        COLL_UCX_VERBOSE(8, _msg " sbuf %p rbuf %p count %i type '%s' comm %d '%s'", \
                __VA_ARGS__, (_sbuf), (_rbuf), (_count), (_datatype)->name, \
                (_comm)->c_contextid, (_comm)->c_name);

static int coll_ucx_allreduce_pre_init(struct ompi_datatype_t *dtype, int count, const void *sbuf,
                                       void *rbuf, char **inplace_buff, ptrdiff_t *gap)
{
    ptrdiff_t dsize, gp, lb = 0;
    char *inpbuf = NULL;
    int err;

    ompi_datatype_type_lb(dtype, &lb);
    if ((dtype->super.flags & OPAL_DATATYPE_FLAG_CONTIGUOUS) &&
        (dtype->super.flags & OPAL_DATATYPE_FLAG_NO_GAPS) &&
        (lb == 0)) {
        return UCS_OK;
    }

    dsize = opal_datatype_span(&dtype->super, count, &gp);
    if (sbuf == MPI_IN_PLACE && dsize != 0) {
        inpbuf = (char *)malloc(dsize);
        if (inpbuf == NULL) {
            return UCS_ERR_NO_MEMORY;
        }
        *inplace_buff = inpbuf;
        *gap = gp;
        err = ompi_datatype_copy_content_same_ddt(dtype, count, inpbuf - gp, (char *)rbuf);
    } else {
        err = ompi_datatype_copy_content_same_ddt(dtype, count, (char *)rbuf, (char *)sbuf);
    }

    return (err == MPI_SUCCESS) ? UCS_OK : UCS_ERR_INVALID_PARAM;
}

int mca_coll_ucx_allreduce(const void *sbuf, void *rbuf, int count,
                           struct ompi_datatype_t *dtype, struct ompi_op_t *op,
                           struct ompi_communicator_t *comm,
                           mca_coll_base_module_t *module)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t *)module;
    char *inplace_buff = NULL;
    ucg_coll_h coll = NULL;
    ptrdiff_t extent, gap = 0;
    char *sbuf_rel = NULL;
    char *request = NULL;

    ompi_datatype_type_extent(dtype, &extent);
    ucs_status_t ret = mca_coll_ucx_check_total_data_size((size_t)extent, count);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_WARN("ucx component only support data size <= 2^32 bytes, fallback to ompi function.");
        goto fallback;
    }

    if (count > 0 && extent > 0) {
        ret = coll_ucx_allreduce_pre_init(dtype, count, sbuf, rbuf, &inplace_buff, &gap);
        if (ret != UCS_OK) {
            goto fallback;
        }
    }

    sbuf_rel = (inplace_buff == NULL) ? (char *)sbuf : inplace_buff - gap;

    COLL_UCX_TRACE("%s", sbuf, rbuf, count, dtype, comm, "allreduce START");

    request = (char *)malloc(mca_coll_ucx_component.request_size);
    if (request == NULL) {
        ret = UCS_ERR_NO_MEMORY;
        goto fallback;
    }
    ucs_status_ptr_t req = request + mca_coll_ucx_component.request_size;

    ret = ucg_coll_allreduce_init(sbuf_rel, rbuf, count, (size_t)extent, dtype, ucx_module->ucg_group, 0,
                                  op, 0, 0, &coll);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_WARN("ucx allreduce init failed: %s, fallback to ompi function.", ucs_status_string(ret));
        goto fallback;
    }

    ret = ucg_collective_start_nbr(coll, req);
    if (OPAL_UNLIKELY(UCS_STATUS_IS_ERR(ret))) {
        COLL_UCX_WARN("ucx allreduce start failed: %s, fallback to ompi function.", ucs_status_string(ret));
        goto fallback;
    }

    if (ucs_unlikely(ret == UCS_OK)) {
        goto fallback;
    }

    ucp_worker_h ucp_worker = mca_coll_ucx_component.ucg_worker;
    MCA_COLL_UCX_WAIT_LOOP(req, ucp_worker, "ucx allreduce", mca_coll_ucx_free((void **)&request));
    COLL_UCX_TRACE("%s", sbuf, rbuf, count, dtype, comm, "allreduce END");

fallback:
    mca_coll_ucx_free((void **)&inplace_buff);
    mca_coll_ucx_free((void **)&request);

    if (ret != UCS_OK) {
        return ucx_module->previous_allreduce(sbuf, rbuf, count, dtype, op,
                                           comm, ucx_module->previous_allreduce_module);
    }

    return OMPI_SUCCESS;
}

#ifdef UCG_COLL_ALREADY_SUPPORTED
int mca_coll_ucx_iallreduce(const void *sbuf, void *rbuf, int count,
                            struct ompi_datatype_t *dtype, struct ompi_op_t *op,
                            struct ompi_communicator_t *comm,
                            struct ompi_request_t **request,
                            mca_coll_base_module_t *module)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*)module;

    COLL_UCX_TRACE("iallreduce request *%p", sbuf, rbuf, count, dtype, comm,
                   (void*)request);

    ptrdiff_t dtype_size;
    ucg_coll_h coll = NULL;
    ompi_datatype_type_extent(dtype, &dtype_size);
    ucs_status_t ret = ucg_coll_allreduce_init(sbuf, rbuf, count, (size_t)dtype_size, dtype, ucx_module->ucg_group,
                                               mca_coll_ucx_coll_completion, op, 0, 0, &coll);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_ERROR("ucx allreduce init failed: %s", ucs_status_string(ret));
        return OMPI_ERROR;
    }

    ompi_request_t *req = (ompi_request_t*)ucg_collective_start_nb(coll);
    if (OPAL_UNLIKELY(UCS_STATUS_IS_ERR(ret))) {
        COLL_UCX_ERROR("ucx allreduce start failed: %s",
                       ucs_status_string(UCS_PTR_STATUS(req)));
        return OMPI_ERROR;
    }

    if (req == NULL) {
        COLL_UCX_VERBOSE(8, "returning completed request");
        *request = &mca_coll_ucx_component.completed_send_req;
        return OMPI_SUCCESS;
    }

    COLL_UCX_VERBOSE(8, "got request %p", (void*)req);
    *request = req;
    return OMPI_SUCCESS;
}

int mca_coll_ucx_allreduce_init(const void *sbuf, void *rbuf, int count,
                                struct ompi_datatype_t *dtype, struct ompi_op_t *op,
                                struct ompi_communicator_t *comm,
                                struct ompi_info_t *info,
                                struct ompi_request_t **request,
                                mca_coll_base_module_t *module)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*)module;

    mca_coll_ucx_persistent_op_t *req =
            (mca_coll_ucx_persistent_op_t *)
            COLL_UCX_FREELIST_GET(&mca_coll_ucx_component.persistent_ops);
    if (OPAL_UNLIKELY(req == NULL)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    COLL_UCX_TRACE("iallreduce_init request *%p=%p",
            sbuf, rbuf, count, dtype, comm, (void*)request, (void*)req);

    ptrdiff_t dtype_size;
    ompi_datatype_type_extent(dtype, &dtype_size);
    ucs_status_t ret = ucg_coll_allreduce_init(sbuf, rbuf, count, (size_t)dtype_size, dtype, ucx_module->ucg_group,
                                               mca_coll_ucx_pcoll_completion, op, 0,
                                               UCG_GROUP_COLLECTIVE_MODIFIER_PERSISTENT, &req->coll_desc);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_ERROR("ucx allreduce failed: %s", ucs_status_string(ret));
        return OMPI_ERROR;
    }

    req->ompi.req_state = OMPI_REQUEST_INACTIVE;
    *request = &req->ompi;
    return OMPI_SUCCESS;
}

int mca_coll_ucx_reduce(const void *sbuf, void* rbuf, int count,
                        struct ompi_datatype_t *dtype, struct ompi_op_t *op,
                        int root, struct ompi_communicator_t *comm,
                        mca_coll_base_module_t *module)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*)module;

    COLL_UCX_TRACE("%s", sbuf, rbuf, count, dtype, comm, "allreduce");

    char *request = (char *)malloc(mca_coll_ucx_component.request_size);
    if (request == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    ucs_status_ptr_t req = request + mca_coll_ucx_component.request_size;

    ptrdiff_t dtype_size;
    ucg_coll_h coll = NULL;
    ompi_datatype_type_extent(dtype, &dtype_size);
    ucs_status_t ret = ucg_coll_reduce_init(sbuf, rbuf, count, (size_t)dtype_size, dtype, ucx_module->ucg_group, 0,
                                            op, root, 0, &coll);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_ERROR("ucx reduce init failed: %s", ucs_status_string(ret));
        goto exit;
    }

    ret = ucg_collective_start_nbr(coll, req);
    if (OPAL_UNLIKELY(UCS_STATUS_IS_ERR(ret))) {
        COLL_UCX_ERROR("ucx reduce start failed: %s", ucs_status_string(ret));
        goto exit;
    }

    if (ucs_unlikely(ret == UCS_OK)) {
        goto exit;
    }

    ucp_worker_h ucp_worker = mca_coll_ucx_component.ucg_worker;
    MCA_COLL_UCX_WAIT_LOOP(req, ucp_worker, "ucx reduce", mca_coll_ucx_free((void **)&request));

exit:
    mca_coll_ucx_free((void **)&request);
    return (ret == UCS_OK) ? OMPI_SUCCESS : OMPI_ERROR;
}

int mca_coll_ucx_scatter(const void *sbuf, int scount, struct ompi_datatype_t *sdtype,
                         void *rbuf, int rcount, struct ompi_datatype_t *rdtype,
                         int root, struct ompi_communicator_t *comm, mca_coll_base_module_t *module)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*)module;

    COLL_UCX_TRACE("%s", sbuf, rbuf, scount, sdtype, comm, "scatter");

    char *request = (char *)malloc(mca_coll_ucx_component.request_size);
    if (request == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    ucs_status_ptr_t req = request + mca_coll_ucx_component.request_size;

    ucg_coll_h coll = NULL;
    ptrdiff_t sdtype_size, rdtype_size;
    ompi_datatype_type_extent(sdtype, &sdtype_size);
    ompi_datatype_type_extent(rdtype, &rdtype_size);
    ucs_status_t ret = ucg_coll_scatter_init(sbuf, scount, (size_t)sdtype_size, sdtype,
                                             rbuf, rcount, (size_t)rdtype_size, rdtype,
                                             ucx_module->ucg_group, 0, 0, root,
                                             0, &coll);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_ERROR("ucx scatter init failed: %s", ucs_status_string(ret));
        goto exit;
    }

    ret = ucg_collective_start_nbr(coll, req);
    if (OPAL_UNLIKELY(UCS_STATUS_IS_ERR(ret))) {
        COLL_UCX_ERROR("ucx scatter start failed: %s", ucs_status_string(ret));
        goto exit;
    }

    if (ucs_unlikely(ret == UCS_OK)) {
        goto exit;
    }

    ucp_worker_h ucp_worker = mca_coll_ucx_component.ucg_worker;
    MCA_COLL_UCX_WAIT_LOOP(req, ucp_worker, "ucx scatter", mca_coll_ucx_free((void **)&request));

exit:
    mca_coll_ucx_free((void **)&request);
    return (ret == UCS_OK) ? OMPI_SUCCESS : OMPI_ERROR;
}

int mca_coll_ucx_gather(const void *sbuf, int scount, struct ompi_datatype_t *sdtype, void *rbuf, int rcount,
                        struct ompi_datatype_t *rdtype, int root, struct ompi_communicator_t *comm,
                        mca_coll_base_module_t *module)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*)module;

    COLL_UCX_TRACE("%s", sbuf, rbuf, scount, sdtype, comm, "gather");

    char *request = (char *)malloc(mca_coll_ucx_component.request_size);
    if (request == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    ucs_status_ptr_t req = request + mca_coll_ucx_component.request_size;

    ucg_coll_h coll = NULL;
    ptrdiff_t sdtype_size, rdtype_size;
    ompi_datatype_type_extent(sdtype, &sdtype_size);
    ompi_datatype_type_extent(rdtype, &rdtype_size);
    ucs_status_t ret = ucg_coll_gather_init(sbuf, scount, (size_t)sdtype_size, sdtype,
                                            rbuf, rcount, (size_t)rdtype_size, rdtype,
                                            ucx_module->ucg_group, 0, 0, root,
                                            0, &coll);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_ERROR("ucx gather init failed: %s", ucs_status_string(ret));
        goto exit;
    }

    ret = ucg_collective_start_nbr(coll, req);
    if (OPAL_UNLIKELY(UCS_STATUS_IS_ERR(ret))) {
        COLL_UCX_ERROR("ucx gather start failed: %s", ucs_status_string(ret));
        goto exit;
    }

    if (ucs_unlikely(ret == UCS_OK)) {
        goto exit;
    }

    ucp_worker_h ucp_worker = mca_coll_ucx_component.ucg_worker;
    MCA_COLL_UCX_WAIT_LOOP(req, ucp_worker, "ucx gather", mca_coll_ucx_free((void **)&request));

exit:
    mca_coll_ucx_free((void **)&request);
    return (ret == UCS_OK) ? OMPI_SUCCESS : OMPI_ERROR;
}

int mca_coll_ucx_allgather(const void *sbuf, int scount, struct ompi_datatype_t *sdtype, void *rbuf, int rcount,
                           struct ompi_datatype_t *rdtype, struct ompi_communicator_t *comm,
                           mca_coll_base_module_t *module)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*)module;

    COLL_UCX_TRACE("%s", sbuf, rbuf, scount, sdtype, comm, "allgather");

    char *request = (char *)malloc(mca_coll_ucx_component.request_size);
    if (request == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    ucs_status_ptr_t req = request + mca_coll_ucx_component.request_size;

    ucg_coll_h coll = NULL;
    ptrdiff_t sdtype_size, rdtype_size;
    ompi_datatype_type_extent(sdtype, &sdtype_size);
    ompi_datatype_type_extent(rdtype, &rdtype_size);
    ucs_status_t ret = ucg_coll_allgather_init(sbuf, scount, (size_t)sdtype_size, sdtype,
                                               rbuf, rcount, (size_t)rdtype_size, rdtype,
                                               ucx_module->ucg_group, 0, 0, 0,
                                               0, &coll);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_ERROR("ucx allgather init failed: %s", ucs_status_string(ret));
        goto exit;
    }

    ret = ucg_collective_start_nbr(coll, req);
    if (OPAL_UNLIKELY(UCS_STATUS_IS_ERR(ret))) {
        COLL_UCX_ERROR("ucx allgather start failed: %s", ucs_status_string(ret));
        goto exit;
    }

    if (ucs_unlikely(ret == UCS_OK)) {
        goto exit;
    }

    ucp_worker_h ucp_worker = mca_coll_ucx_component.ucg_worker;
    MCA_COLL_UCX_WAIT_LOOP(req, ucp_worker, "ucx allgather", mca_coll_ucx_free((void **)&request));

exit:
    mca_coll_ucx_free((void **)&request);
    return (ret == UCS_OK) ? OMPI_SUCCESS : OMPI_ERROR;
}

int mca_coll_ucx_alltoall(const void *sbuf, int scount, struct ompi_datatype_t *sdtype, void *rbuf, int rcount,
                          struct ompi_datatype_t *rdtype, struct ompi_communicator_t *comm,
                          mca_coll_base_module_t *module)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*)module;

    COLL_UCX_TRACE("%s", sbuf, rbuf, scount, sdtype, comm, "alltoall");

    char *request = (char *)malloc(mca_coll_ucx_component.request_size);
    if (request == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    ucs_status_ptr_t req = request + mca_coll_ucx_component.request_size;

    ucg_coll_h coll = NULL;
    ptrdiff_t sdtype_size, rdtype_size;
    ompi_datatype_type_extent(sdtype, &sdtype_size);
    ompi_datatype_type_extent(rdtype, &rdtype_size);
    ucs_status_t ret = ucg_coll_alltoall_init(sbuf, scount, (size_t)sdtype_size, sdtype,
                                              rbuf, rcount, (size_t)rdtype_size, rdtype,
                                              ucx_module->ucg_group, 0, 0, 0,
                                              0, &coll);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_ERROR("ucx alltoall init failed: %s", ucs_status_string(ret));
        goto exit;
    }

    ret = ucg_collective_start_nbr(coll, req);
    if (OPAL_UNLIKELY(UCS_STATUS_IS_ERR(ret))) {
        COLL_UCX_ERROR("ucx alltoall start failed: %s", ucs_status_string(ret));
        goto exit;
    }

    if (ucs_unlikely(ret == UCS_OK)) {
        goto exit;
    }

    ucp_worker_h ucp_worker = mca_coll_ucx_component.ucg_worker;
    MCA_COLL_UCX_WAIT_LOOP(req, ucp_worker, "ucx alltoall", mca_coll_ucx_free((void **)&request));

exit:
    mca_coll_ucx_free((void **)&request);
    return (ret == UCS_OK) ? OMPI_SUCCESS : OMPI_ERROR;
}
#endif /* UCG_COLL_ALREADY_SUPPORTED */

int mca_coll_ucx_alltoallv(const void *sbuf, const int *scounts, const int *sdispls, struct ompi_datatype_t *sdtype,
                           void *rbuf, const int *rcounts, const int *rdispls, struct ompi_datatype_t *rdtype,
                           struct ompi_communicator_t *comm, mca_coll_base_module_t *module)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t *)module;

    COLL_UCX_TRACE("%s", sbuf, rbuf, *scounts, sdtype, comm, "alltoallv");

    char *request = NULL;
    ucs_status_t ret = UCS_ERR_UNSUPPORTED;

    int size = ompi_comm_size(comm);
    ptrdiff_t sdtype_size, rdtype_size;
    ompi_datatype_type_extent(sdtype, &sdtype_size);
    ompi_datatype_type_extent(rdtype, &rdtype_size);

    /*
     * current alltoallv can not support non-contig datatype and large datatype,
     * fallback to original ompi alltoallv
     */

    /* non-contig datatype */
    unsigned is_send_contig, is_recv_contig;
    is_send_contig = mca_coll_ucg_check_contig_datatype(sdtype);
    is_recv_contig = mca_coll_ucg_check_contig_datatype(rdtype);

    if (!is_send_contig || !is_recv_contig) {
        COLL_UCX_WARN("current hmpi alltoallv cannot support non-contig datatype, fallback to ompi function.");
        goto fallback;
    }

    /* large datatype */
    if (sdtype_size > LARGE_DATATYPE_THRESHOLD || rdtype_size > LARGE_DATATYPE_THRESHOLD) {
        COLL_UCX_WARN("current hmpi alltoallv cannot support large datatype, fallback to ompi function.");
        goto fallback;
    }

    int total_send_count = 0;
    int total_recv_count = 0;

    /* The send displs of alltoallv may not increase. */
    int i;
    for (i = 0; i < size; i++) {
        total_send_count += scounts[i];
    }
    ret = mca_coll_ucx_check_total_data_size((size_t)sdtype_size, total_send_count);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_WARN("ucx component only support data size <= 2^32 bytes, fallback to ompi function.");
        goto fallback;
    }

    /* The recv displs of alltoallv may not increase. */
    for (i = 0; i < size; i++) {
        total_recv_count += rcounts[i];
    }
    ret = mca_coll_ucx_check_total_data_size((size_t)rdtype_size, total_recv_count);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_WARN("ucx component only support data size <= 2^32 bytes, fallback to ompi function.");
        goto fallback;
    }

    ucg_coll_h coll = NULL;
    ret = ucg_coll_alltoallv_init(sbuf, scounts, (size_t)sdtype_size, sdtype, sdispls,
                                  rbuf, rcounts, (size_t)rdtype_size, rdtype, rdispls,
                                  ucx_module->ucg_group, 0, 0, 0, 0, &coll);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_WARN("ucx alltoallv init failed: %s", ucs_status_string(ret));
        goto fallback;
    }

    request = (char *)malloc(mca_coll_ucx_component.request_size);
    if (request == NULL) {
        ret = UCS_ERR_NO_MEMORY;
        goto fallback;
    }
    ucs_status_ptr_t req = request + mca_coll_ucx_component.request_size;
    ret = ucg_collective_start_nbr(coll, req);
    if (OPAL_UNLIKELY(UCS_STATUS_IS_ERR(ret))) {
        COLL_UCX_WARN("ucx alltoallv start failed: %s", ucs_status_string(ret));
        goto fallback;
    }

    if (ucs_unlikely(ret == UCS_OK)) {
        goto fallback;
    }

    ucp_worker_h ucp_worker = mca_coll_ucx_component.ucg_worker;
    MCA_COLL_UCX_WAIT_LOOP(req, ucp_worker, "ucx alltoallv", mca_coll_ucx_free((void **)&request));

fallback:
    mca_coll_ucx_free((void **)&request);
    if (ret != UCS_OK) {
        return ucx_module->previous_alltoallv(sbuf, scounts, sdispls, sdtype,
                                               rbuf, rcounts, rdispls, rdtype,
                                               comm, ucx_module->previous_alltoallv_module);
    }
    return OMPI_SUCCESS;
}

int mca_coll_ucx_barrier(struct ompi_communicator_t *comm, mca_coll_base_module_t *module)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*)module;

    char *request = (char *)malloc(mca_coll_ucx_component.request_size);
    if (request == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    ucs_status_ptr_t req = request + mca_coll_ucx_component.request_size;

    ucg_coll_h coll = NULL;
    ucs_status_t ret = ucg_coll_barrier_init(0, ucx_module->ucg_group, 0, 0, 0, 0, &coll);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_WARN("ucx barrier init failed: %s, fallback to ompi function.", ucs_status_string(ret));
        goto fallback;
    }

    ret = ucg_collective_start_nbr(coll, req);
    if (OPAL_UNLIKELY(UCS_STATUS_IS_ERR(ret))) {
        COLL_UCX_WARN("ucx barrier start failed: %s, fallback to ompi function.", ucs_status_string(ret));
        goto fallback;
    }

    if (ucs_unlikely(ret == UCS_OK)) {
        goto fallback;
    }

    ucp_worker_h ucp_worker = mca_coll_ucx_component.ucg_worker;
    MCA_COLL_UCX_WAIT_LOOP(req, ucp_worker, "ucx barrier", mca_coll_ucx_free((void **)&request));

fallback:
    mca_coll_ucx_free((void **)&request);
    if (ret != UCS_OK) {
        return ucx_module->previous_barrier(comm, ucx_module->previous_barrier_module);
    }
    return OMPI_SUCCESS;
}

int mca_coll_ucx_bcast(void *buff, int count, struct ompi_datatype_t *dtype, int root,
                       struct ompi_communicator_t *comm, mca_coll_base_module_t *module)
{
    mca_coll_ucx_module_t *ucx_module = (mca_coll_ucx_module_t*)module;

    COLL_UCX_TRACE("%s", buff, buff, count, dtype, comm, "bcast");

    char *request = (char *)malloc(mca_coll_ucx_component.request_size);
    if (request == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    ucs_status_ptr_t req = request + mca_coll_ucx_component.request_size;
    ptrdiff_t dtype_size;
    ucg_coll_h coll = NULL;
    ompi_datatype_type_extent(dtype, &dtype_size);
    ucs_status_t ret = mca_coll_ucx_check_total_data_size((size_t)dtype_size, count);
    if (OPAL_UNLIKELY(ret != UCS_OK)) {
        COLL_UCX_WARN("ucx component only support data size <= 2^32 bytes, fallback to ompi function.");
        goto fallback;
    }
    ret = ucg_coll_bcast_init(buff, buff, count, (size_t)dtype_size, dtype, ucx_module->ucg_group, 0,
                              0, root, 0, &coll);
    if (OPAL_UNLIKELY(UCS_STATUS_IS_ERR(ret))) {
        COLL_UCX_WARN("ucx bcast init failed: %s, fallback to ompi function.", ucs_status_string(ret));
        goto fallback;
    }

    ret = ucg_collective_start_nbr(coll, req);
    if (OPAL_UNLIKELY(UCS_STATUS_IS_ERR(ret))) {
        COLL_UCX_WARN("ucx bcast start failed: %s, fallback to ompi function.", ucs_status_string(ret));
        goto fallback;
    }

    if (ucs_unlikely(ret == UCS_OK)) {
        goto fallback;
    }

    ucp_worker_h ucp_worker = mca_coll_ucx_component.ucg_worker;
    MCA_COLL_UCX_WAIT_LOOP(req, ucp_worker, "ucx bcast", mca_coll_ucx_free((void **)&request));

fallback:
    mca_coll_ucx_free((void **)&request);
    if (ret != UCS_OK) {
        return ucx_module->previous_bcast(buff, count, dtype, root,
                                           comm, ucx_module->previous_bcast_module);
    }
    return OMPI_SUCCESS;
}
