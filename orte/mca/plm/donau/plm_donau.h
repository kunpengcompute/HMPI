/*
 * Copyright (c) 2024      Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_PLM_DONAU_EXPORT_H
#define ORTE_PLM_DONAU_EXPORT_H

#include "orte_config.h"

#include "orte/mca/mca.h"
#include "orte/mca/plm/plm.h"
BEGIN_C_DECLS

struct orte_plm_donau_component_t {
    orte_plm_base_component_t super;
    char *custom_args;
    bool donau_warning_msg;
};
typedef struct orte_plm_donau_component_t orte_plm_donau_component_t;

/*
 * Globally exported variable
 */

ORTE_MODULE_DECLSPEC extern orte_plm_donau_component_t mca_plm_donau_component;
ORTE_DECLSPEC extern orte_plm_base_module_t orte_plm_donau_module;

END_C_DECLS

#endif /* ORTE_PLM_DONAU_EXPORT_H */