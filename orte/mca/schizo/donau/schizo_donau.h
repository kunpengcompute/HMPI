/*
 * Copyright (c) 2024      Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _MCA_SCHIZO_DONAU_H_
#define _MCA_SCHIZO_DONAU_H_

#include "orte_config.h"

#include "orte/types.h"

#include "opal/mca/base/base.h"
#include "orte/mca/schizo/schizo.h"


BEGIN_C_DECLS

ORTE_MODULE_DECLSPEC extern orte_schizo_base_component_t mca_schizo_donau_component;
extern orte_schizo_base_module_t orte_schizo_donau_module;

END_C_DECLS

#endif /* _MCA_SCHIZO_DONAU_H_ */