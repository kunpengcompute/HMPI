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
        if (NULL == node) {
            num_nodes = 0;
            opal_output_verbose(10, orte_ras_base_framework.framework_output,
                                "ras/donau: Failed when create obj of orte_node_t");
            goto cleanup;
        }
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
cleanup:
    if (NULL != nodes) {
        OPAL_LIST_RELEASE(nodes);
    }
    free(line);
    fclose(fp);
    return num_nodes;
}

static int orte_ras_donau_allocate(orte_job_t *jdata, opal_list_t *nodes) {
    int num_nodes;
    char *alloc_path = NULL;

    /* get the list of allocated nodes */
    alloc_path = getenv("CCS_ALLOC_FILE");
    if (NULL == alloc_path || 0 == strlen(alloc_path) ||
       ((num_nodes = donau_get_alloc(alloc_path, nodes))) <= 0) {
        orte_show_help("help-ras-donau.txt", "nodelist-failed", true);
        return ORTE_ERR_NOT_AVAILABLE;
    }

    return ORTE_SUCCESS;
}

static int orte_ras_donau_finalize(void)
{
    return ORTE_SUCCESS;
}