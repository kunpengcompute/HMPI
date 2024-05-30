/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2022-2024 Huawei Technologies Co., Ltd.
 *                                All rights reserved.
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


static int mca_coll_ucg_request_allgatherv_init(mca_coll_ucg_req_t *coll_req,
                                                const void *sbuf, int scount,
                                                ompi_datatype_t *sdtype,
                                                void *rbuf, const int *rcounts,
                                                const int *disps,
                                                ompi_datatype_t *rdtype,
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

    ucg_request_h ucg_req;
    ucg_status_t status = ucg_request_allgatherv_init(sbuf, scount, ucg_send_dt,
                                                      rbuf, rcounts, disps, ucg_recv_dt,
                                                      module->group, &coll_req->info,
                                                      nb, &ucg_req);
    if (status != UCG_OK) {
        UCG_DEBUG("Failed to initialize ucg request, %s", ucg_status_string(status));
        return OMPI_ERROR;
    }
    coll_req->ucg_req = ucg_req;
    return OMPI_SUCCESS;
}

int mca_coll_ucg_allgatherv(const void *sbuf, int scount, ompi_datatype_t *sdtype,
                            void *rbuf, const int *rcounts, const int *disps,
                            ompi_datatype_t *rdtype, ompi_communicator_t *comm,
                            mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg allgatherv");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;
    mca_coll_ucg_req_t coll_req;
    OBJ_CONSTRUCT(&coll_req, mca_coll_ucg_req_t);
    int rc;
    rc = mca_coll_ucg_request_common_init(&coll_req, false, false);
    if (rc != OMPI_SUCCESS) {
        goto fallback;
    }

    rc = mca_coll_ucg_request_allgatherv_init(&coll_req, sbuf, scount, sdtype,
                                              rbuf, rcounts, disps, rdtype,
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
    UCG_DEBUG("fallback allgatherv");
    return ucg_module->previous_allgatherv(sbuf, scount, sdtype, rbuf, rcounts,
                                           disps, rdtype, comm,
                                           ucg_module->previous_allgatherv_module);
}

int mca_coll_ucg_allgatherv_cache(const void *sbuf, int scount, ompi_datatype_t *sdtype,
                                  void *rbuf, const int *rcounts, const int *disps,
                                  ompi_datatype_t *rdtype, ompi_communicator_t *comm,
                                  mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg allgatherv cache");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;
    mca_coll_ucg_args_t args = {
        .coll_type = MCA_COLL_UCG_TYPE_ALLGATHERV,
        .comm = comm,
        .allgatherv.sbuf = sbuf,
        .allgatherv.scount = scount,
        .allgatherv.sdtype = sdtype,
        .allgatherv.rbuf = rbuf,
        .allgatherv.rcounts = rcounts,
        .allgatherv.disps = disps,
        .allgatherv.rdtype = rdtype,
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

    MCA_COLL_UCG_REQUEST_PATTERN(&args, mca_coll_ucg_request_allgatherv_init,
                                 sbuf, scount, sdtype, rbuf, rcounts, disps,
                                 rdtype, ucg_module, UCG_REQUEST_BLOCKING);
    return OMPI_SUCCESS;

fallback:
    UCG_DEBUG("fallback allgatherv");
    return ucg_module->previous_allgatherv(sbuf, scount, sdtype, rbuf, rcounts,
                                           disps, rdtype, comm,
                                           ucg_module->previous_allgatherv_module);
}

int mca_coll_ucg_iallgatherv(const void *sbuf, int scount, ompi_datatype_t *sdtype,
                             void *rbuf, const int *rcounts, const int *disps,
                             ompi_datatype_t *rdtype, ompi_communicator_t *comm,
                             ompi_request_t **request, mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg iallgatherv");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;

    int rc;
    mca_coll_ucg_req_t *coll_req = mca_coll_ucg_rpool_get();
    rc = mca_coll_ucg_request_common_init(coll_req, true, false);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }

    rc = mca_coll_ucg_request_allgatherv_init(coll_req, sbuf, scount, sdtype,
                                              rbuf, rcounts, disps, rdtype,
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
    UCG_DEBUG("fallback iallgatherv");
    return ucg_module->previous_iallgatherv(sbuf, scount, sdtype, rbuf, rcounts,
                                            disps, rdtype, comm, request,
                                            ucg_module->previous_iallgatherv_module);
}

int mca_coll_ucg_iallgatherv_cache(const void *sbuf, int scount, ompi_datatype_t *sdtype,
                                   void *rbuf, const int *rcounts, const int *disps,
                                   ompi_datatype_t *rdtype, ompi_communicator_t *comm,
                                   ompi_request_t **request, mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg iallgatherv cache");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;
    mca_coll_ucg_args_t args = {
        .coll_type = MCA_COLL_UCG_TYPE_IALLGATHERV,
        .comm = comm,
        .allgatherv.sbuf = sbuf,
        .allgatherv.scount = scount,
        .allgatherv.sdtype = sdtype,
        .allgatherv.rbuf = rbuf,
        .allgatherv.rcounts = rcounts,
        .allgatherv.disps = disps,
        .allgatherv.rdtype = rdtype,
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

    MCA_COLL_UCG_REQUEST_PATTERN_NB(request, &args, mca_coll_ucg_request_allgatherv_init,
                                    sbuf, scount, sdtype, rbuf, rcounts, disps,
                                    rdtype, ucg_module, UCG_REQUEST_NONBLOCKING);
    return OMPI_SUCCESS;

fallback:
    UCG_DEBUG("fallback iallgatherv");
    return ucg_module->previous_iallgatherv(sbuf, scount, sdtype, rbuf, rcounts,
                                            disps, rdtype, comm, request,
                                            ucg_module->previous_iallgatherv_module);
}

int mca_coll_ucg_allgatherv_init(const void *sbuf, int scount, ompi_datatype_t *sdtype,
                                 void *rbuf, const int *rcounts, const int *disps,
                                 ompi_datatype_t *rdtype, ompi_communicator_t *comm,
                                 ompi_info_t *info, ompi_request_t **request,
                                 mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg allgatherv init");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;

    int rc;
    mca_coll_ucg_req_t *coll_req = mca_coll_ucg_rpool_get();
    rc = mca_coll_ucg_request_common_init(coll_req, false, true);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }

    rc = mca_coll_ucg_request_allgatherv_init(coll_req, sbuf, scount, sdtype,
                                              rbuf, rcounts, disps, rdtype,
                                              ucg_module, UCG_REQUEST_BLOCKING);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_request_cleanup(coll_req);
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }

    *request = &coll_req->super.super;
    return OMPI_SUCCESS;

fallback:
    UCG_DEBUG("fallback allgatherv init");
    return ucg_module->previous_allgatherv_init(sbuf, scount, sdtype, rbuf, rcounts,
                                                disps, rdtype, comm, info, request,
                                                ucg_module->previous_allgatherv_module);
}