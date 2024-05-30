/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2022-2024 Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * HEADER$
 */
#include "coll_ucg.h"
#include "coll_ucg_request.h"
#include "coll_ucg_debug.h"
#include "coll_ucg_dt.h"


static int mca_coll_ucg_request_alltoallv_init(mca_coll_ucg_req_t *coll_req,
                                               const void *sbuf, const int *scounts,
                                               const int *sdispls, ompi_datatype_t *sdtype,
                                               void *rbuf, const int *rcounts,
                                               const int *rdispls, ompi_datatype_t *rdtype,
                                               mca_coll_ucg_module_t *module,
                                               ucg_request_type_t nb)
{
    ucg_dt_h ucg_send_dt;
    int rc = mca_coll_ucg_type_adapt(sdtype, &ucg_send_dt, NULL, NULL);
    if (rc != OMPI_SUCCESS) {
        UCG_DEBUG("Failed to adapt type");
        return rc;
    }

    ucg_dt_h ucg_recv_dt;
    rc = mca_coll_ucg_type_adapt(rdtype, &ucg_recv_dt, NULL, NULL);
    if (rc != OMPI_SUCCESS) {
        UCG_DEBUG("Failed to adapt type");
        return rc;
    }

    // TODO: Check the memory type of buffer if possible
    ucg_request_h ucg_req;
    const void *tmp_sbuf = sbuf == MPI_IN_PLACE ? UCG_IN_PLACE : sbuf;
    ucg_status_t status = ucg_request_alltoallv_init(tmp_sbuf, scounts, sdispls, ucg_send_dt,
                                                     rbuf, rcounts, rdispls, ucg_recv_dt,
                                                     module->group, &coll_req->info,
                                                     nb, &ucg_req);
    if (status != UCG_OK) {
        UCG_DEBUG("Failed to initialize ucg request, %s", ucg_status_string(status));
        return OMPI_ERROR;
    }
    coll_req->ucg_req = ucg_req;
    return OMPI_SUCCESS;
}

int mca_coll_ucg_alltoallv(const void *sbuf, const int *scounts, const int *sdispls,
                           ompi_datatype_t *sdtype, void *rbuf, const int *rcounts,
                           const int *rdispls, ompi_datatype_t *rdtype,
                           ompi_communicator_t *comm, mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg alltoallv");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;
    mca_coll_ucg_req_t coll_req;
    OBJ_CONSTRUCT(&coll_req, mca_coll_ucg_req_t);
    int rc;
    rc = mca_coll_ucg_request_common_init(&coll_req, false, false);
    if (rc != OMPI_SUCCESS) {
        goto fallback;
    }

    rc = mca_coll_ucg_request_alltoallv_init(&coll_req,
                                             sbuf, scounts, sdispls, sdtype,
                                             rbuf, rcounts, rdispls, rdtype,
                                             ucg_module, UCG_REQUEST_BLOCKING);
    if (rc != OMPI_SUCCESS) {
        goto fallback;
    }

    rc = mca_coll_ucg_request_execute(&coll_req);
    mca_coll_ucg_request_cleanup(&coll_req);
    if (rc != OMPI_SUCCESS) {
        goto fallback;
    }

    OBJ_DESTRUCT(&coll_req);
    return OMPI_SUCCESS;

fallback:
    OBJ_DESTRUCT(&coll_req);
    UCG_DEBUG("fallback alltoallv");
    return ucg_module->previous_alltoallv(sbuf, scounts, sdispls, sdtype,
                                          rbuf, rcounts, rdispls, rdtype,
                                          comm, ucg_module->previous_alltoallv_module);
}

int mca_coll_ucg_alltoallv_cache(const void *sbuf, const int *scounts, const int *sdispls,
                                 ompi_datatype_t *sdtype, void *rbuf, const int *rcounts,
                                 const int *rdispls, ompi_datatype_t *rdtype,
                                 ompi_communicator_t *comm, mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg alltoallv cache");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;
    mca_coll_ucg_args_t args = {
        .coll_type = MCA_COLL_UCG_TYPE_ALLTOALLV,
        .comm = comm,
        .alltoallv.sbuf = sbuf,
        .alltoallv.scounts = scounts,
        .alltoallv.sdispls = sdispls,
        .alltoallv.sdtype = sdtype,
        .alltoallv.rbuf = rbuf,
        .alltoallv.rcounts = rcounts,
        .alltoallv.rdispls = rdispls,
        .alltoallv.rdtype = rdtype,
    };

    int rc;
    rc = mca_coll_ucg_request_execute_cache(&args);
    if (rc == OMPI_SUCCESS) {
        return rc;
    }

    if (rc != OMPI_ERR_NOT_FOUND) {
        /* The failure may is caused by a UCG internal error. Retry may also fail
           and should do fallback immediately. */
        goto fallback;
    }

    MCA_COLL_UCG_REQUEST_PATTERN(&args, mca_coll_ucg_request_alltoallv_init,
                                 sbuf, scounts, sdispls, sdtype,
                                 rbuf, rcounts, rdispls, rdtype,
                                 ucg_module, UCG_REQUEST_BLOCKING);
    return OMPI_SUCCESS;

fallback:
    UCG_DEBUG("fallback alltoallv");
    return ucg_module->previous_alltoallv(sbuf, scounts, sdispls, sdtype,
                                          rbuf, rcounts, rdispls, rdtype,
                                          comm, ucg_module->previous_alltoallv_module);
}

int mca_coll_ucg_ialltoallv(const void *sbuf, const int *scounts, const int *sdispls,
                            ompi_datatype_t *sdtype, void *rbuf, const int *rcounts,
                            const int *rdispls, ompi_datatype_t *rdtype,
                            ompi_communicator_t *comm, ompi_request_t **request,
                            mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg ialltoallv");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;

    int rc;
    mca_coll_ucg_req_t *coll_req = mca_coll_ucg_rpool_get();
    rc = mca_coll_ucg_request_common_init(coll_req, true, false);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }

    rc = mca_coll_ucg_request_alltoallv_init(coll_req,
                                             sbuf, scounts, sdispls, sdtype,
                                             rbuf, rcounts, rdispls, rdtype,
                                             ucg_module, UCG_REQUEST_NONBLOCKING);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_request_cleanup(coll_req);
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }

    rc = mca_coll_ucg_request_execute_nb(coll_req);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_request_cleanup(coll_req);
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }
    *request = &coll_req->super.super;
    return OMPI_SUCCESS;
fallback:
    UCG_DEBUG("fallback ialltoallv");
    return ucg_module->previous_ialltoallv(sbuf, scounts, sdispls, sdtype,
                                           rbuf, rcounts, rdispls, rdtype,
                                           comm, request,
                                           ucg_module->previous_ialltoallv_module);
}

int mca_coll_ucg_ialltoallv_cache(const void *sbuf, const int *scounts, const int *sdispls,
                                  ompi_datatype_t *sdtype, void *rbuf, const int *rcounts,
                                  const int *rdispls, ompi_datatype_t *rdtype,
                                  ompi_communicator_t *comm, ompi_request_t **request,
                                  mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg ialltoallv cache");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;
    mca_coll_ucg_args_t args = {
        .coll_type = MCA_COLL_UCG_TYPE_IALLTOALLV,
        .comm = comm,
        .alltoallv.sbuf = sbuf,
        .alltoallv.scounts = scounts,
        .alltoallv.sdispls = sdispls,
        .alltoallv.sdtype = sdtype,
        .alltoallv.rbuf = rbuf,
        .alltoallv.rcounts = rcounts,
        .alltoallv.rdispls = rdispls,
        .alltoallv.rdtype = rdtype,
    };

    int rc;
    mca_coll_ucg_req_t *coll_req = NULL;
    rc = mca_coll_ucg_request_execute_cache_nb(&args, &coll_req);
    if (rc == OMPI_SUCCESS) {
        *request = &coll_req->super.super;
        return rc;
    }

    if (rc != OMPI_ERR_NOT_FOUND) {
        /* The failure may is caused by a UCG internal error. Retry may also fail
           and should do fallback immediately. */
        goto fallback;
    }

    MCA_COLL_UCG_REQUEST_PATTERN_NB(request, &args, mca_coll_ucg_request_alltoallv_init,
                                    sbuf, scounts, sdispls, sdtype,
                                    rbuf, rcounts, rdispls, rdtype,
                                    ucg_module, UCG_REQUEST_NONBLOCKING);
    return OMPI_SUCCESS;

fallback:
    UCG_DEBUG("fallback ialltoallv");
    return ucg_module->previous_ialltoallv(sbuf, scounts, sdispls, sdtype,
                                           rbuf, rcounts, rdispls, rdtype,
                                           comm, request,
                                           ucg_module->previous_ialltoallv_module);
}

int mca_coll_ucg_alltoallv_init(const void *sbuf, const int *scounts, const int *sdispls,
                                ompi_datatype_t *sdtype, void *rbuf, const int *rcounts,
                                const int *rdispls, ompi_datatype_t *rdtype,
                                ompi_communicator_t *comm, ompi_info_t *info,
                                ompi_request_t **request, mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg alltoallv init");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;

    int rc;
    mca_coll_ucg_req_t *coll_req = mca_coll_ucg_rpool_get();
    rc = mca_coll_ucg_request_common_init(coll_req, false, true);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }

    rc = mca_coll_ucg_request_alltoallv_init(coll_req,
                                             sbuf, scounts, sdispls, sdtype,
                                             rbuf, rcounts, rdispls, rdtype,
                                             ucg_module, UCG_REQUEST_BLOCKING);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_request_cleanup(coll_req);
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }

    *request = &coll_req->super.super;
    return OMPI_SUCCESS;

fallback:
    UCG_DEBUG("fallback alltoallv init");
    return ucg_module->previous_alltoallv_init(sbuf, scounts, sdispls, sdtype,
                                               rbuf, rcounts, rdispls, rdtype,
                                               comm, info, request,
                                               ucg_module->previous_alltoallv_module);
}