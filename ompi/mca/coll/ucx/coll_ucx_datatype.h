/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2020. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef COLL_UCX_DATATYPE_H_
#define COLL_UCX_DATATYPE_H_

#include "coll_ucx.h"


#define COLL_UCX_DATATYPE_INVALID   0

#define UCP_DT_IS_CONTIG(_datatype) \
    (((_datatype) & UCP_DATATYPE_CLASS_MASK) == UCP_DATATYPE_CONTIG)

#define LARGE_DATATYPE_THRESHOLD 32

#ifdef HAVE_UCP_REQUEST_PARAM_T
typedef struct {
    ucp_datatype_t          datatype;
    int                     size_shift;
} coll_ucx_datatype_t;
#endif

struct coll_ucx_convertor {
    opal_free_list_item_t   super;
    ompi_datatype_t         *datatype;
    opal_convertor_t        opal_conv;
    size_t                  offset;
};

ucp_datatype_t mca_coll_ucx_init_datatype(ompi_datatype_t *datatype);

int mca_coll_ucx_datatype_attr_del_fn(ompi_datatype_t* datatype, int keyval,
                                     void *attr_val, void *extra);

OBJ_CLASS_DECLARATION(mca_coll_ucx_convertor_t);


__opal_attribute_always_inline__
static inline ucp_datatype_t mca_coll_ucx_get_datatype(ompi_datatype_t *datatype)
{
#ifdef HAVE_UCP_REQUEST_PARAM_T
    coll_ucx_datatype_t *ucp_type = (coll_ucx_datatype_t*)datatype->pml_data;

    if (OPAL_LIKELY(ucp_type != COLL_UCX_DATATYPE_INVALID)) {
        return ucp_type->datatype;
    }
#else
    ucp_datatype_t ucp_type = datatype->pml_data;

    if (OPAL_LIKELY(ucp_type != COLL_UCX_DATATYPE_INVALID)) {
        return ucp_type;
    }
#endif

    return mca_coll_ucx_init_datatype(datatype);
}

#ifdef HAVE_UCP_REQUEST_PARAM_T
__opal_attribute_always_inline__
static inline coll_ucx_datatype_t*
mca_coll_ucx_get_op_data(ompi_datatype_t *datatype)
{
    coll_ucx_datatype_t *ucp_type = (coll_ucx_datatype_t*)datatype->pml_data;

    if (OPAL_LIKELY(ucp_type != COLL_UCX_DATATYPE_INVALID)) {
        return ucp_type;
    }

    mca_coll_ucx_init_datatype(datatype);
    return (coll_ucx_datatype_t*)datatype->pml_data;
}

__opal_attribute_always_inline__
static inline size_t mca_coll_ucx_get_data_size(coll_ucx_datatype_t *op_data,
                                               size_t count)
{
    return count << op_data->size_shift;
}
#endif

__opal_attribute_always_inline__
static int mca_coll_ucg_check_contig_datatype(ompi_datatype_t *mpi_dt)
{
    ucp_datatype_t ucp_datatype = mca_coll_ucx_get_datatype(mpi_dt);

    return UCP_DT_IS_CONTIG(ucp_datatype);
}

#endif /* COLL_UCX_DATATYPE_H_ */
