/*
 * Copyright (c) 2024      Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "orte/mca/schizo/schizo.h"
#include "orte_config.h"
#include "orte/types.h"
#include "opal/types.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <ctype.h>

#include "opal/util/argv.h"
#include "opal/util/basename.h"
#include "opal/util/opal_environ.h"

#include "orte/runtime/orte_globals.h"
#include "orte/util/name_fns.h"
#include "orte/mca/schizo/base/base.h"

#include "schizo_donau.h"

static orte_schizo_launch_environ_t check_launch_environment(void);
static int get_remaining_time(uint32_t *timeleft);
static void finalize(void);

orte_schizo_base_module_t orte_schizo_donau_module = {
    .check_launch_environment = check_launch_environment,
    .get_remaining_time = get_remaining_time,
    .finalize = finalize
};

static char **pushed_envs = NULL;
static char **pushed_vals = NULL;
static orte_schizo_launch_environ_t myenv;
static bool myenvdefined = false;

static orte_schizo_launch_environ_t check_launch_environment(void)
{
    int i;

    if (myenvdefined) {
        return myenv;
    }
    myenvdefined = true;

    /* we were only selected because DONAU was detected
     * and we are an app, so no need to further check
     * that here. Instead, see if we were direct launched
     * vs launched via mpirun */
    if (NULL != orte_process_info.my_daemon_uri) {
        /* nope */
        myenv = ORTE_SCHIZO_NATIVE_LAUNCHED;
        opal_argv_append_nosize(&pushed_envs, OPAL_MCA_PREFIX"ess");
        opal_argv_append_nosize(&pushed_vals, "pmi");
        /* mark that we are native */
        opal_argv_append_nosize(&pushed_envs, "ORTE_SCHIZO_DETECTION");
        opal_argv_append_nosize(&pushed_vals, "NATIVE");

        goto setup;
    }

    /* see if we are in a DONAU allocation */
    char *donau_alloc_file = getenv("CCS_ALLOC_FILE");
    if (NULL == donau_alloc_file || 0 == strlen(donau_alloc_file)) {
        /* nope */
        myenv = ORTE_SCHIZO_UNDETERMINED;
        return myenv;
    }

    /* mark that we are in DONAU */
    opal_argv_append_nosize(&pushed_envs, "ORTE_SCHIZO_DETECTION");
    opal_argv_append_nosize(&pushed_vals, "DONAU");

    /* we are in an allocation, but were we direct launched
     * or are we a singleton? */
    char *donau_step_id = getenv("CCS_STEP_ID");
    if (NULL == donau_step_id || 0 == strlen(donau_step_id)) {
        /* not in a job step - ensure we select the
         * correct things */
        opal_argv_append_nosize(&pushed_envs, OPAL_MCA_PREFIX"ess");
        opal_argv_append_nosize(&pushed_vals, "singleton");
        myenv = ORTE_SCHIZO_MANAGED_SINGLETON;
        goto setup;
    }

    myenv = ORTE_SCHIZO_DIRECT_LAUNCHED;
    opal_argv_append_nosize(&pushed_envs, OPAL_MCA_PREFIX"ess");
    opal_argv_append_nosize(&pushed_vals, "pmi");

    /* if we are direct-launched by DONAU, then disable binding */
    opal_argv_append_nosize(&pushed_envs, OPAL_MCA_PREFIX"hwloc_base_binding_policy");
    opal_argv_append_nosize(&pushed_vals, "none");
    /* indicate we are externally bound so we won't try to do it ourselves */
    opal_argv_append_nosize(&pushed_envs, OPAL_MCA_PREFIX"orte_externally_bound");
    opal_argv_append_nosize(&pushed_vals, "1");

  setup:
      opal_output_verbose(1, orte_schizo_base_framework.framework_output,
                          "schizo:donau DECLARED AS %s", orte_schizo_base_print_env(myenv));
    if (NULL != pushed_envs) {
        for (i=0; NULL != pushed_envs[i]; i++) {
            opal_setenv(pushed_envs[i], pushed_vals[i], true, &environ);
        }
    }
    return myenv;
}

static int get_remaining_time(uint32_t *timeleft)
{
    /* set the default, TODO */
    *timeleft = UINT32_MAX;

    return ORTE_SUCCESS;
}
static void finalize(void)
{
    int i;

    if (NULL != pushed_envs) {
        for (i=0; NULL != pushed_envs[i]; i++) {
            opal_unsetenv(pushed_envs[i], &environ);
        }
        opal_argv_free(pushed_envs);
        opal_argv_free(pushed_vals);
    }
}