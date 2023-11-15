/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2022-2023 Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * HEADER$
 */
#include "ompi_config.h"
#include "coll_ucg.h"
#include "coll_ucg_dt.h"
#include "coll_ucg_debug.h"
#include "coll_ucg_request.h"

#include "opal/util/argv.h"

/*
 * Public string showing the coll ompi_ucg component version number
 */
const char *mca_coll_ucg_component_version_string =
  "Open MPI UCG collective MCA component version " OMPI_VERSION;

/*
 * Global variable
 */
int mca_coll_ucg_output = -1;

/*
 * Local function
 */
static int mca_coll_ucg_register(void);
static int mca_coll_ucg_open(void);
static int mca_coll_ucg_close(void);

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

mca_coll_ucg_component_t mca_coll_ucg_component = {
    /* First, fill in the super */
    {
        /* First, the mca_component_t struct containing meta information
           about the component itself */
        .collm_version = {
#if OMPI_MAJOR_VERSION > 4
            MCA_COLL_BASE_VERSION_2_4_0,
#else
            MCA_COLL_BASE_VERSION_2_0_0,
#endif

            /* Component name and version */
            .mca_component_name = "ucg",
            MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION,
                                  OMPI_RELEASE_VERSION),

            /* Component open and close functions */
            .mca_open_component = mca_coll_ucg_open,
            .mca_close_component = mca_coll_ucg_close,
            .mca_register_component_params = mca_coll_ucg_register,
        },
        .collm_data = {
            /* The component is not checkpoint ready */
            MCA_BASE_METADATA_PARAM_NONE
        },

        /* Initialization / querying functions */
        .collm_init_query = mca_coll_ucg_init_query,
        .collm_comm_query = mca_coll_ucg_comm_query,
    },
    .initialized = false,
    /* MCA parameter */
    .priority = 90,             /* priority */
    .verbose = 2,               /* verbose level */
    .max_rcache_size = 10,
    .disable_coll = NULL,
    .topology = NULL,

    .ucg_context = NULL,
    //TODO: More parameters should be added below.
};

static int mca_coll_ucg_register(void)
{
    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "priority",
                                          "Priority of the UCG component",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_6,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_coll_ucg_component.priority);

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "verbose",
                                          "Verbosity of the UCG component, "
                                          "0:fatal, 1:error, 2:warn, 3:info, 4:debug, >4:fine-grained trace logs",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_9,
                                          MCA_BASE_VAR_SCOPE_LOCAL,
                                          &mca_coll_ucg_component.verbose);

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "max_rcache_size",
                                          "Max size of request cache",
                                          MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                          OPAL_INFO_LVL_6,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_coll_ucg_component.max_rcache_size);

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "disable_coll",
                                          "Comma separated list of collective operations to disable",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_9,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_coll_ucg_component.disable_coll);

    (void)mca_base_component_var_register(&mca_coll_ucg_component.super.collm_version, "topology",
                                          "Path of the topology file required by the net-topo-aware algorithm",
                                          MCA_BASE_VAR_TYPE_STRING, NULL, 0, 0,
                                          OPAL_INFO_LVL_9,
                                          MCA_BASE_VAR_SCOPE_READONLY,
                                          &mca_coll_ucg_component.topology);

    return OMPI_SUCCESS;
}

static int mca_coll_ucg_open(void)
{
    mca_coll_ucg_component_t *cm = &mca_coll_ucg_component;
    mca_coll_ucg_output = opal_output_open(NULL);
    opal_output_set_verbosity(mca_coll_ucg_output, cm->verbose);
    return OMPI_SUCCESS;
}

static int mca_coll_ucg_close(void)
{
    /* In some cases, mpi_comm_world is not the last comm to free.
     * call mca_coll_ucg_cleanup_once here, ensure cleanup ucg resources at last.
     */
    mca_coll_ucg_cleanup_once();
    return OMPI_SUCCESS;
}