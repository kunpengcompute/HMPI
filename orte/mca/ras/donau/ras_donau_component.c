/*
 * Copyright (c) 2024      Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/base/base.h"
#include "opal/util/net.h"
#include "opal/opal_socket_errno.h"

#include "orte/util/name_fns.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/ras/base/ras_private.h"
#include "ras_donau.h"

/*
 * Local functions
 */
static int ras_donau_register(void);
static int ras_donau_open(void);
static int ras_donau_close(void);
static int orte_ras_donau_component_query(mca_base_module_t **module, int *priority);


orte_ras_donau_component_t mca_ras_donau_component = {
    {
        /* First, the mca_base_component_t struct containing meta
         * information about the component itself
         */

        .base_version = {
            ORTE_RAS_BASE_VERSION_2_0_0,

            /* Component name and version */
            .mca_component_name = "donau",
            MCA_BASE_MAKE_VERSION(component, ORTE_MAJOR_VERSION, ORTE_MINOR_VERSION,
                                ORTE_RELEASE_VERSION),

            /* Component open and close functions */
            .mca_open_component = ras_donau_open,
            .mca_close_component = ras_donau_close,
            .mca_query_component = orte_ras_donau_component_query,
            .mca_register_component_params = ras_donau_register
        },
        .base_data = {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        },
    }
};

static int ras_donau_register(void)
{
    mca_base_component_t *component = &mca_ras_donau_component.super.base_version;

    mca_ras_donau_component.param_priority = 100;
    (void) mca_base_component_var_register (component,
                                            "priority", "Priority of the donau ras component",
                                            MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                            OPAL_INFO_LVL_9,
                                            MCA_BASE_VAR_SCOPE_READONLY,
                                            &mca_ras_donau_component.param_priority);

    return ORTE_SUCCESS;
}
static int ras_donau_open(void)
{
    return ORTE_SUCCESS;
}

static int ras_donau_close(void)
{
    return ORTE_SUCCESS;
}

static int orte_ras_donau_component_query(mca_base_module_t **module, int *priority)
{
    /* check if donau is running here */
   char *donau_job_id = getenv("CCS_JOB_ID");
   if (NULL == donau_job_id || 0 == strlen(donau_job_id) || 0 == orte_donau_launch_type) {
        /* disqualify ourselves */
        *priority = 0;
        *module = NULL;
        return ORTE_ERROR;
    }

    OPAL_OUTPUT_VERBOSE((2, orte_base_framework.framework_output,
                         "%s ras:slurm: available for selection",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
    /* since only one RM can exist on a cluster, just set
     * my priority to something - the other components won't
     * be responding anyway
     */
    *priority = mca_ras_donau_component.param_priority;
    *module = (mca_base_module_t *)&orte_ras_donau_module;
    return ORTE_SUCCESS;   
}