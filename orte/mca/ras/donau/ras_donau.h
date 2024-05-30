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
/**
 * @file
 *
 * Resource Allocation(DONAU)
 */
#ifndef ORTE_RAS_DONAU_H
#define ORTE_RAS_DONAU_H

#include "orte_config.h"
#include "orte/mca/ras/ras.h"
#include "orte/mca/ras/base/base.h"

BEGIN_C_DECLS

/**
 * RAS Component
 */
typedef struct {
    orte_ras_base_component_t super;
    int param_priority;
} orte_ras_donau_component_t;

ORTE_DECLSPEC extern orte_ras_donau_component_t mca_ras_donau_component;
ORTE_DECLSPEC extern orte_ras_base_module_t orte_ras_donau_module;

END_C_DECLS

#endif