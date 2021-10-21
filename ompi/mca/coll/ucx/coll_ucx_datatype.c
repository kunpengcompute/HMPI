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

#include "coll_ucx_datatype.h"
#include "coll_ucx_request.h"

#include "ompi/runtime/mpiruntime.h"
#include "ompi/attribute/attribute.h"

#include <inttypes.h>
#include <math.h>

static void* coll_ucx_generic_datatype_start_pack(void *context, const void *buffer,
                                                  size_t count)
{
    ompi_datatype_t *datatype = context;
    mca_coll_ucx_convertor_t *convertor;

    convertor = (mca_coll_ucx_convertor_t *)COLL_UCX_FREELIST_GET(&mca_coll_ucx_component.convs);

    OMPI_DATATYPE_RETAIN(datatype);
    convertor->datatype = datatype;
    opal_convertor_copy_and_prepare_for_send(ompi_proc_local_proc->super.proc_convertor,
                                             &datatype->super, count, buffer, 0,
                                             &convertor->opal_conv);
    return convertor;
}

static void* coll_ucx_generic_datatype_start_unpack(void *context, void *buffer,
                                                   size_t count)
{
    ompi_datatype_t *datatype = context;
    mca_coll_ucx_convertor_t *convertor;

    convertor = (mca_coll_ucx_convertor_t *)COLL_UCX_FREELIST_GET(&mca_coll_ucx_component.convs);

    OMPI_DATATYPE_RETAIN(datatype);
    convertor->datatype = datatype;
    convertor->offset = 0;
    opal_convertor_copy_and_prepare_for_recv(ompi_proc_local_proc->super.proc_convertor,
                                             &datatype->super, count, buffer, 0,
                                             &convertor->opal_conv);
    return convertor;
}

static size_t coll_ucx_generic_datatype_packed_size(void *state)
{
    mca_coll_ucx_convertor_t *convertor = state;
    size_t size;

    opal_convertor_get_packed_size(&convertor->opal_conv, &size);
    return size;
}

static size_t coll_ucx_generic_datatype_pack(void *state, size_t offset,
                                             void *dest, size_t max_length)
{
    mca_coll_ucx_convertor_t *convertor = state;
    uint32_t iov_count;
    struct iovec iov;
    size_t length;
    int rc;

    iov_count    = 1;
    iov.iov_base = dest;
    iov.iov_len  = max_length;

    opal_convertor_set_position(&convertor->opal_conv, &offset);
    length = max_length;
    rc = opal_convertor_pack(&convertor->opal_conv, &iov, &iov_count, &length);
    if (OPAL_UNLIKELY(rc < 0)) {
        int err = MPI_ERR_INTERN;
        COLL_UCX_ERROR("Failed to pack datatype structure");
        ompi_mpi_errors_are_fatal_comm_handler(NULL, &err, "Failed to pack datatype structure");
    } else {
        return length;
    }
}

static ucs_status_t coll_ucx_generic_datatype_unpack(void *state, size_t offset,
                                                     const void *src, size_t length)
{
    mca_coll_ucx_convertor_t *convertor = state;
    int rc;
    uint32_t iov_count;
    struct iovec iov;
    opal_convertor_t conv;

    iov_count    = 1;
    iov.iov_base = (void*)src;
    iov.iov_len  = length;

    /* in case if unordered message arrived - create separate convertor to
     * unpack data. */
    if (offset != convertor->offset) {
        OBJ_CONSTRUCT(&conv, opal_convertor_t);
        opal_convertor_copy_and_prepare_for_recv(ompi_proc_local_proc->super.proc_convertor,
                                                 &convertor->datatype->super,
                                                 convertor->opal_conv.count,
                                                 convertor->opal_conv.pBaseBuf, 0,
                                                 &conv);
        opal_convertor_set_position(&conv, &offset);
        rc = opal_convertor_unpack(&conv, &iov, &iov_count, &length);
        opal_convertor_cleanup(&conv);
        OBJ_DESTRUCT(&conv);
        /* permanently switch to un-ordered mode */
        convertor->offset = 0;
    } else {
        rc = opal_convertor_unpack(&convertor->opal_conv, &iov, &iov_count, &length);
        convertor->offset += length;
    }
    if (OPAL_UNLIKELY(rc < 0)) {
        int err = MPI_ERR_INTERN;
        COLL_UCX_ERROR("Failed to unpack datatype structure");
        ompi_mpi_errors_are_fatal_comm_handler(NULL, &err, "Failed to unpack datatype structure");
    } else {
        return UCS_OK;
    }
}

static void coll_ucx_generic_datatype_finish(void *state)
{
    mca_coll_ucx_convertor_t *convertor = state;

    opal_convertor_cleanup(&convertor->opal_conv);
    OMPI_DATATYPE_RELEASE(convertor->datatype);
    COLL_UCX_FREELIST_RETURN(&mca_coll_ucx_component.convs, &convertor->super);
}

static ucp_generic_dt_ops_t coll_ucx_generic_datatype_ops = {
    .start_pack   = coll_ucx_generic_datatype_start_pack,
    .start_unpack = coll_ucx_generic_datatype_start_unpack,
    .packed_size  = coll_ucx_generic_datatype_packed_size,
    .pack         = coll_ucx_generic_datatype_pack,
    .unpack       = coll_ucx_generic_datatype_unpack,
    .finish       = coll_ucx_generic_datatype_finish
};

int mca_coll_ucx_datatype_attr_del_fn(ompi_datatype_t* datatype, int keyval,
                                      void *attr_val, void *extra)
{
    ucp_datatype_t ucp_datatype = (ucp_datatype_t)attr_val;

#ifdef HAVE_UCP_REQUEST_PARAM_T
    free((void*)datatype->coll_data);
#else
    COLL_UCX_ASSERT((uint64_t)ucp_datatype == datatype->coll_data);
#endif
    ucp_dt_destroy(ucp_datatype);
    datatype->coll_data = COLL_UCX_DATATYPE_INVALID;
    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__
static inline int mca_coll_ucx_datatype_is_contig(ompi_datatype_t *datatype)
{
    ptrdiff_t lb;

    ompi_datatype_type_lb(datatype, &lb);

    return (datatype->super.flags & OPAL_DATATYPE_FLAG_CONTIGUOUS) &&
           (datatype->super.flags & OPAL_DATATYPE_FLAG_NO_GAPS) &&
           (lb == 0);
}

#ifdef HAVE_UCP_REQUEST_PARAM_T
__opal_attribute_always_inline__ static inline
coll_ucx_datatype_t *mca_coll_ucx_init_nbx_datatype(ompi_datatype_t *datatype,
                                                    ucp_datatype_t ucp_datatype,
                                                    size_t size)
{
    coll_ucx_datatype_t *coll_datatype;
    int is_contig_pow2;

    coll_datatype = malloc(sizeof(*coll_datatype));
    if (coll_datatype == NULL) {
        int err = MPI_ERR_INTERN;
        COLL_UCX_ERROR("Failed to allocate datatype structure");
        ompi_mpi_errors_are_fatal_comm_handler(NULL, &err, "Failed to allocate datatype structure");
    }

    coll_datatype->datatype = ucp_datatype;

    is_contig_pow2 = mca_coll_ucx_datatype_is_contig(datatype) &&
                     (size && !(size & (size - 1))); /* is_pow2(size) */
    if (is_contig_pow2) {
        coll_datatype->size_shift = (int)(log(size) / log(2.0)); /* log2(size) */
    } else {
        coll_datatype->size_shift = 0;
    }

    return coll_datatype;
}
#endif

ucp_datatype_t mca_coll_ucx_init_datatype(ompi_datatype_t *datatype)
{
    size_t size = 0; /* init to suppress compiler warning */
    ucp_datatype_t ucp_datatype;
    ucs_status_t status;
    int ret;

    if (mca_coll_ucx_datatype_is_contig(datatype)) {
        ompi_datatype_type_size(datatype, &size);
        ucp_datatype = ucp_dt_make_contig(size);
        goto out;
    }

    status = ucp_dt_create_generic(&coll_ucx_generic_datatype_ops,
                                   datatype, &ucp_datatype);
    if (status != UCS_OK) {
        int err = MPI_ERR_INTERN;
        COLL_UCX_ERROR("Failed to create UCX datatype for %s", datatype->name);
        ompi_mpi_errors_are_fatal_comm_handler(NULL, &err, "Failed to allocate datatype structure");
    }

    /* Add custom attribute, to clean up UCX resources when OMPI datatype is
     * released.
     */
    if (ompi_datatype_is_predefined(datatype)) {
        COLL_UCX_ASSERT(datatype->id < OMPI_DATATYPE_MAX_PREDEFINED);
        mca_coll_ucx_component.predefined_types[datatype->id] = ucp_datatype;
    } else {
        ret = ompi_attr_set_c(TYPE_ATTR, datatype, &datatype->d_keyhash,
                              mca_coll_ucx_component.datatype_attr_keyval,
                              (void*)ucp_datatype, false);
        if (ret != OMPI_SUCCESS) {
            int err = MPI_ERR_INTERN;
            COLL_UCX_ERROR("Failed to add UCX datatype attribute for %s (%p): %d",
                           datatype->name, (void*)datatype, ret);
            ompi_mpi_errors_are_fatal_comm_handler(NULL, &err, "Failed to allocate datatype structure");
        }
    }
out:
    COLL_UCX_VERBOSE(7, "created generic UCX datatype 0x%"PRIx64, ucp_datatype)

#ifdef HAVE_UCP_REQUEST_PARAM_T
    UCS_STATIC_ASSERT(sizeof(datatype->coll_data) >= sizeof(coll_ucx_datatype_t*));
    datatype->coll_data = (uint64_t)mca_coll_ucx_init_nbx_datatype(datatype,
                                                                  ucp_datatype,
                                                                  size);
#else
    datatype->coll_data = ucp_datatype;
#endif

    return ucp_datatype;
}

static void mca_coll_ucx_convertor_construct(mca_coll_ucx_convertor_t *convertor)
{
    OBJ_CONSTRUCT(&convertor->opal_conv, opal_convertor_t);
}

static void mca_coll_ucx_convertor_destruct(mca_coll_ucx_convertor_t *convertor)
{
    OBJ_DESTRUCT(&convertor->opal_conv);
}

OBJ_CLASS_INSTANCE(mca_coll_ucx_convertor_t,
                   opal_free_list_item_t,
                   mca_coll_ucx_convertor_construct,
                   mca_coll_ucx_convertor_destruct);
