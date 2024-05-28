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
#include "orte/mca/mca.h"
#include "orte/mca/ess/ess.h"

#ifndef ORTE_ESS_DONAU_H
#define ORTE_ESS_DONAU_H

BEGIN_C_DECLS

ORTE_MODULE_DECLSPEC extern orte_ess_base_component_t mca_ess_donau_component;

/*
 * Module open / close
 */
int orte_ess_donau_component_open(void);
int orte_ess_donau_component_close(void);
int orte_ess_donau_component_query(mca_base_module_t **module, int *priority);

END_C_DECLS

#endif /* ORTE_ESS_DONAU_H */