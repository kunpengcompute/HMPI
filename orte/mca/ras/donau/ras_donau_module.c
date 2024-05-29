/*
 * Copyright (c) 2024      Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal/class/opal_object.h"
#include "opal/dss/dss_types.h"
#include "opal/util/output.h"
#include "orte/constants.h"
#include "orte/util/attr.h"
#include "orte_config.h"

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include "opal/mca/hwloc/hwloc-internal.h"
#include "opal/util/argv.h"
#include "opal/util/net.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/rmaps/rmaps_types.h"
#include "orte/runtime/orte_globals.h"
#include "orte/util/show_help.h"

#include "orte/mca/ras/base/base.h"
#include "orte/mca/ras/base/ras_private.h"
#include "ras_donau.h"

/*
 * Local functions
 */
static int orte_ras_donau_allocate(orte_job_t *jdata, opal_list_t *nodes);
static int orte_ras_donau_finalize(void);
static int donau_get_alloc(char *alloc_path, opal_list_t *nodes);
static void donau_get_affinity(char *affinity_path, char *affinity_file);

/*
 * RAS donau module
 */
orte_ras_base_module_t orte_ras_donau_module = {
    NULL,
    orte_ras_donau_allocate,
    NULL,
    orte_ras_donau_finalize
};

static int donau_get_alloc(char *alloc_path, opal_list_t *nodes)
{
    int num_nodes = 0;
    orte_node_t *node = NULL;
    FILE *fp;
    fp = fopen(alloc_path, "r");
    if (NULL == fp) {
        return num_nodes;
    }
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    while ((read = getline(&line, &len, fp)) != -1) {
        char hostname[DONAU_MAX_NODENAME_LENGTH] = {0};
        int num_kernels = 0;
        int slots = 0;
        if (sscanf(line, "%s %d %d", hostname, &num_kernels, &slots) != 3) {
            opal_output_verbose(10, orte_ras_base_framework.framework_output,
                                "ras/donau: Get the wrong num of params in CCS_ALLOC_FILE");
            break;
        }

        node = OBJ_NEW(orte_node_t);
        node->name = strdup(hostname);
        node->state = ORTE_NODE_STATE_UP;
        node->slots_inuse = 0;
        node->slots_max = 0;
        node->slots = slots;
        opal_list_append(nodes, &node->super);
        num_nodes++;
    }
    free(line);
    fclose(fp);
    return num_nodes;
}

static void donau_get_affinity(char *affinity_path, char *affinity_file)
{
    FILE *fp;
    fp = fopen(alloc_path, "r");
    if (NULL == fp) {
        return ;
    }
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    while ((read = getline(&line, &len, fp)) != -1) {
        int rank_ID = -1;
        char hostname[DONAU_MAX_NODENAME_LENGTH] = {0};
        char physical_index[DONAU_MAX_NODENAME_LENGTH] = {0};
        char logical_index[DONAU_MAX_NODENAME_LENGTH] = {0};
        if (sscanf(line, "%d %s %s %s", &rank_ID, hostname, physical_index, logical_index) != 4) {
            opal_output_verbose(10, orte_ras_base_framework.framework_output,
                                "ras/donau: Get the wrong num of params in CCS_COSCHED_MPI_AFFINITY_FILE");
            break;
        }

        snprintf(affinity_file + strlen(affinity_file), DONAU_MAX_NODELIST_LENGTH - strlen(affinity_file),
                 "rank %d=%s slot=%s\n", rank_ID, hostname, logical_index);
    }
    free(line);
    fclose(fp);
    return;
}

static int orte_ras_donau_allocate(orte_job_t *jdata, opal_list_t *nodes) {
    int num_nodes;
    char *alloc_path = NULL;
    char *affinity_path = NULL;
    char *affinity_file = NULL;
    struct stat buf;
    bool directives_given = false;

    /* get the list of allocated nodes */
    alloc_path = getenv("CCS_ALLOC_FILE");
    if (NULL == alloc_path || 0 == strlen(alloc_path) ||
       ((num_nodes = donau_get_alloc(alloc_path, nodes))) <= 0) {
        orte_show_help("help-ras-donau.txt", "nodelist-failed", true);
        return ORTE_ERR_NOT_AVAILABLE;
    }

    /* check to see if any mapping or binding directives were given */
    if (NULL != jdata && NULL != jdata->map) {
        if ((ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(jdata->map->mapping)) ||
            OPAL_BINDING_POLICY_IS_SET(jdata->map->binding)) {
            directives_given = true;
        } else if ((ORTE_MAPPING_GIVEN & ORTE_GET_MAPPING_DIRECTIVE(orte_rmaps_base.mapping)) ||
            OPAL_BINDING_POLICY_IS_SET(opal_hwloc_binding_policy)) {
            directives_given = true;
        }
    }

    /* check for an affinity file */
    affinity_path = getenv("CCS_COSCHED_MPI_AFFINITY_TILE");
    if (!directives_given && NULL != affinity_path && 0 != strlen(affinity_path)){
        /* check to see if the file is empty - if it is,
         * then affinity wasn't actually set for this job
         */
        affinity_file = NULL;
        donau_get_affinity(affinity_path, affinity_file);
        if (0 != stat(affinity_file, &buf)) {
            orte_show_help("help-ras-donau.txt", "addinity-file-not-found", true, affinity_file);
            return ORTE_ERR_SILENT;
        }
        if (0 == buf.st_size) {
            /* no affinity, so just return */
            return ORTE_SUCCESS;
        }
        /* the affinity file sequentially lists rank location, with
         * cpusets given as physical cpu-ids. Setup the job object
         * so it knows to process this accordingly.
         */
        if (NULL == jdata->map) {
            jdata->map = OBJ_NEW(orte_job_map_t);
        }
        ORTE_SET_MAPPING_POLICY(jdata->map->mapping, ORTE_MAPPING_SEQ);
        jdata->map->req_mapper = strdup("seq"); // need sequential mapper
        /* tell the sequential mapper that all cpusets are to be treated as "physical" */
        orte_set_attribute(&jdata->attributes, ORTE_JOB_PHYSICAL_CPUIDS, true, NULL, OPAL_BOOL);
        /* DONAU provides its info as hwthreads, so set the hwthread-as-cpu flag */
        opal_hwloc_use_hwthreads_as_cpu = true;
        /* don't override something provided bt the user, but default to bind-to hwthread*/
        if (!OPAL_BINDING_POLICY_IS_SET(opal_hwloc_binding_policy)) {
            OPAL_SET_BINDING_POLICY(opal_hwloc_binding_policy, OPAL_BIND_TO_HWTHREAD);
        }
        /*
         * Do not set the hostfile attribute on each app_context since that
         * would confuse the sequential mapper when it tries to assign bindings
         * when running an MPMD job.
         * Instead just overwrite the orte_default_hostfile so it will be
         * general for all of the app_contexts.
         */
        if (NULL != orte_default_hostfile) {
            free(orte_default_hostfile);
            orte_default_hostfile = NULL;
        }
        orte_default_hostfile = strdup(affinity_file);
        opal_output_verbose(10, orte_ras_base_framework.framework_output,
                            "ras/donau: Set default_hostfile to %s", orte_default_hostfile);

        return ORTE_SUCCESS;
    }

    return ORTE_SUCCESS;
}
