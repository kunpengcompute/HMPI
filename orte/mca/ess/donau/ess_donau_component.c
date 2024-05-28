/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2024      Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 * 
 * These symbols are in a file by themselves to provide nice linker
 * semantics. Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire component just to query their version and paramters.
*/

#include "orte_config.h"
#include "orte/constants.h"

#include "orte/util/proc_info.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/ess/ess.h"
#include "orte/mca/ess/donau/ess_donau.h"
#include <stdlib.h>

extern orte_ess_base_module_t orte_ess_donau_module;

/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

orte_ess_base_component_t mca_ess_donau_component = {
    .base_version = {
        ORTE_ESS_BASE_VERSION_3_0_0,

        /* Component name and version */
        .mca_component_name = "donau",
        MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                              ORTE_RELEASE_VERSION),

        /* Component open and close functions */
        .mca_open_component = orte_ess_donau_component_open,
        .mca_close_component = orte_ess_donau_component_close,
        .mca_query_component = orte_ess_donau_component_query,
    },
    .base_data = {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

int orte_ess_donau_component_open(void)
{
    return ORTE_SUCCESS;
}

int orte_ess_donau_component_query(mca_base_module_t **module, int *priority)
{
    /* Are we running under a DONAU job? Were
     * we given a path back to the HNP? If the
     * answer to both is "yes", then we were launched
     * by mpirun in a donau world, so make ourselves available
     */
   char *donau_job_id = getenv("CCS_JOB_ID");
   if (ORTE_PROC_IS_DAEMON &&
       NULL != donau_job_id &&
       0 != strlen(donau_job_id) &&
       NULL != orte_process_info.my_hnp_uri &&
       1 == orte_donau_launch_type) {
        *priority = 100;
        *module = (mca_base_module_t *)&orte_ess_donau_module;
        return ORTE_SUCCESS;
    }

    /*Sadly, no */
    *priority = -1;
    *module = NULL;
    return ORTE_ERROR;   
}

int orte_ess_donau_component_close(void)
{
    return ORTE_SUCCESS;
}