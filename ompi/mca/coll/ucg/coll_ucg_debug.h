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
#ifndef MCA_COLL_UCG_DEBUG_H
#define MCA_COLL_UCG_DEBUG_H

#include "ompi_config.h"
#pragma GCC system_header

#ifdef __BASE_FILE__
#define __UCG_FILE__ __BASE_FILE__
#else
#define __UCG_FILE__ __FILE__
#endif

#define UCG_FATAL(_format, ... ) \
    opal_output_verbose(0, mca_coll_ucg_output, "[%s:%d] FATAL " _format, \
                        __UCG_FILE__, __LINE__, ## __VA_ARGS__); \
    abort()

#define UCG_ERROR(_format, ... ) \
    opal_output_verbose(1, mca_coll_ucg_output, "[%s:%d] ERROR " _format, \
                        __UCG_FILE__, __LINE__, ## __VA_ARGS__)

#define UCG_WARN(_format, ... ) \
    opal_output_verbose(2, mca_coll_ucg_output, "[%s:%d] WARN  " _format, \
                        __UCG_FILE__, __LINE__, ## __VA_ARGS__)

#define UCG_INFO(_format, ... ) \
    opal_output_verbose(3, mca_coll_ucg_output, "[%s:%d] INFO  " _format, \
                        __UCG_FILE__, __LINE__, ## __VA_ARGS__)

#define UCG_INFO_IF(_cond, _format, ... ) \
    if (_cond) { \
        opal_output_verbose(3, mca_coll_ucg_output, "[%s:%d] INFO  " _format, \
                            __UCG_FILE__, __LINE__, ## __VA_ARGS__); \
    }

#define UCG_DEBUG(_format, ... ) \
    opal_output_verbose(4, mca_coll_ucg_output, "[%s:%d] DEBUG " _format, \
                        __UCG_FILE__, __LINE__, ## __VA_ARGS__)

extern int mca_coll_ucg_output;
#endif //MCA_COLL_UCG_DEBUG_H