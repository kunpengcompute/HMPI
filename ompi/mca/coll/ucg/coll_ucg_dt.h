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
#ifndef MCA_COLL_UCG_DT_H
#define MCA_COLL_UCG_DT_H

#include "coll_ucg.h"


typedef struct {
    opal_free_list_t flist;
} mca_coll_ucg_conv_pool_t;

typedef struct mca_coll_ucg_convertor {
    opal_free_list_item_t   super;
    ompi_datatype_t         *datatype;
    opal_convertor_t        opal_conv;
    size_t                  offset;
} mca_coll_ucg_convertor_t;
OBJ_CLASS_DECLARATION(mca_coll_ucg_convertor_t);

typedef struct mca_coll_ucg_type_table {
    ucg_dt_h predefined_dt[UCG_DT_TYPE_PREDEFINED_LAST];
    ucg_op_h predefined_op[UCG_OP_TYPE_PREDEFINED_LAST];
    int attr_key; /* key of saving user-defined ucg dt */
} mca_coll_ucg_type_table_t;

/* Initialize the convertor pool */
int mca_coll_ucg_conv_pool_init(void);
/* Cleanup the convertor pool */
void mca_coll_ucg_conv_pool_cleanup(void);

/**
 * @brief Initialize ucg type.
 *
 * It depends on ompi attr, should be invoked after ompi_attr_init()
 */
int mca_coll_ucg_type_init(void);
/* Cleanup ucg type */
void mca_coll_ucg_type_cleanup(void);

void mca_coll_ucg_type_free_kv(void);
/**
 * @brief Adapt ompi type to ucg type.
 *
 * For operations such as allreduce, *ucg_op should point to a memory space
 * of size UCG_OP_SIZE.
 */
int mca_coll_ucg_type_adapt(ompi_datatype_t *ompi_dt, ucg_dt_h *ucg_dt,
                            ompi_op_t *ompi_op, ucg_op_h *ucg_op);

#endif //MCA_COLL_UCG_DT_H