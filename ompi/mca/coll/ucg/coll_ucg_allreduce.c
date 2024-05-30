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

static int mca_coll_ucg_request_allreduce_init(mca_coll_ucg_req_t *coll_req,
                                               const void *sbuf, void *rbuf, int count,
                                               ompi_datatype_t *datatype, ompi_op_t *op,
                                               mca_coll_ucg_module_t *module,
                                               ucg_request_type_t nb)
{
    /* Trick: Prepare sufficient space for storing ucg_op_h. */
    char tmp[UCG_OP_SIZE];
    ucg_dt_h ucg_dt;
    ucg_op_h ucg_op = (ucg_op_h)tmp;
    int rc = mca_coll_ucg_type_adapt(datatype, &ucg_dt, op, &ucg_op);
    if (rc != OMPI_SUCCESS) {
        UCG_DEBUG("Failed to adapt type");
        return rc;
    }

    // TODO: Check the memory type of buffer if possible
    ucg_request_h ucg_req;
    const void *tmp_sbuf = sbuf == MPI_IN_PLACE ? UCG_IN_PLACE : sbuf;
    ucg_status_t status = ucg_request_allreduce_init(tmp_sbuf, rbuf, count,
                                                     ucg_dt, ucg_op, module->group,
                                                     &coll_req->info, nb, &ucg_req);
    if (status != UCG_OK) {
        UCG_DEBUG("Failed to initialize ucg request, %s", ucg_status_string(status));
        return OMPI_ERROR;
    }
    coll_req->ucg_req = ucg_req;
    return OMPI_SUCCESS;
}

int mca_coll_ucg_allreduce(const void *sbuf, void *rbuf, int count,
                           ompi_datatype_t *datatype, ompi_op_t *op,
                           ompi_communicator_t *comm,
                           mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg allreduce");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;
    mca_coll_ucg_req_t coll_req;
    OBJ_CONSTRUCT(&coll_req, mca_coll_ucg_req_t);
    int rc;
    rc = mca_coll_ucg_request_common_init(&coll_req, false, false);
    if (rc != OMPI_SUCCESS) {
        goto fallback;
    }

    rc = mca_coll_ucg_request_allreduce_init(&coll_req, sbuf, rbuf, count, datatype,
                                             op, ucg_module, UCG_REQUEST_BLOCKING);
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
    UCG_DEBUG("fallback allreduce");
    return ucg_module->previous_allreduce(sbuf, rbuf, count, datatype, op, comm,
                                          ucg_module->previous_allreduce_module);
}

int mca_coll_ucg_allreduce_cache(const void *sbuf, void *rbuf, int count,
                                 ompi_datatype_t *datatype, ompi_op_t *op,
                                 ompi_communicator_t *comm,
                                 mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg allreduce cache");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;
    mca_coll_ucg_args_t args = {
        .coll_type = MCA_COLL_UCG_TYPE_ALLREDUCE,
        .comm = comm,
        .allreduce.sbuf = sbuf,
        .allreduce.rbuf = rbuf,
        .allreduce.count = count,
        .allreduce.datatype = datatype,
        .allreduce.op = op,
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

    MCA_COLL_UCG_REQUEST_PATTERN(&args, mca_coll_ucg_request_allreduce_init,
                                 sbuf, rbuf, count, datatype, op,
                                 ucg_module, UCG_REQUEST_BLOCKING);
    return OMPI_SUCCESS;

fallback:
    UCG_DEBUG("fallback allreduce");
    return ucg_module->previous_allreduce(sbuf, rbuf, count, datatype, op, comm,
                                          ucg_module->previous_allreduce_module);
}

int mca_coll_ucg_iallreduce(const void *sbuf, void *rbuf, int count,
                            ompi_datatype_t *datatype, ompi_op_t *op,
                            ompi_communicator_t *comm, ompi_request_t **request,
                            mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg iallreduce");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;

    int rc;
    mca_coll_ucg_req_t *coll_req = mca_coll_ucg_rpool_get();
    rc = mca_coll_ucg_request_common_init(coll_req, true, false);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }

    rc = mca_coll_ucg_request_allreduce_init(coll_req, sbuf, rbuf, count, datatype, op,
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
    UCG_DEBUG("fallback iallreduce");
    return ucg_module->previous_iallreduce(sbuf, rbuf, count, datatype, op, comm,
                                           request, ucg_module->previous_iallreduce_module);
}

int mca_coll_ucg_iallreduce_cache(const void *sbuf, void *rbuf, int count,
                                  ompi_datatype_t *datatype, ompi_op_t *op,
                                  ompi_communicator_t *comm, ompi_request_t **request,
                                  mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg iallreduce cache");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;
    mca_coll_ucg_args_t args = {
        .coll_type = MCA_COLL_UCG_TYPE_IALLREDUCE,
        .comm = comm,
        .allreduce.sbuf = sbuf,
        .allreduce.rbuf = rbuf,
        .allreduce.count = count,
        .allreduce.datatype = datatype,
        .allreduce.op = op,
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

    MCA_COLL_UCG_REQUEST_PATTERN_NB(request, &args, mca_coll_ucg_request_allreduce_init,
                                    sbuf, rbuf, count, datatype, op, ucg_module,
                                    UCG_REQUEST_NONBLOCKING);
    return OMPI_SUCCESS;

fallback:
    UCG_DEBUG("fallback iallreduce");
    return ucg_module->previous_iallreduce(sbuf, rbuf, count, datatype, op, comm,
                                           request, ucg_module->previous_iallreduce_module);
}

int mca_coll_ucg_allreduce_init(const void *sbuf, void *rbuf, int count, ompi_datatype_t *datatype,
                                ompi_op_t *op, ompi_communicator_t *comm, ompi_info_t *info,
                                ompi_request_t **request, mca_coll_base_module_t *module)
{
    UCG_DEBUG("ucg allreduce init");

    mca_coll_ucg_module_t *ucg_module = (mca_coll_ucg_module_t*)module;

    int rc;
    mca_coll_ucg_req_t *coll_req = mca_coll_ucg_rpool_get();
    rc = mca_coll_ucg_request_common_init(coll_req, false, true);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }

    rc = mca_coll_ucg_request_allreduce_init(coll_req, sbuf, rbuf, count, datatype, op,
                                             ucg_module, UCG_REQUEST_BLOCKING);
    if (rc != OMPI_SUCCESS) {
        mca_coll_ucg_request_cleanup(coll_req);
        mca_coll_ucg_rpool_put(coll_req);
        goto fallback;
    }

    *request = &coll_req->super.super;
    return OMPI_SUCCESS;

fallback:
    UCG_DEBUG("fallback allreduce init");
    return ucg_module->previous_allreduce_init(sbuf, rbuf, count, datatype, op, comm, info,
                                               request, ucg_module->previous_allreduce_module);
}