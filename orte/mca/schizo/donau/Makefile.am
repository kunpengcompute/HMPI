#
# Copyright (c) 2024      Huawei Technologies Co., Ltd.
#                         All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

sources = \
        schizo_donau_component.c \
        schizo_donau.h \
        schizo_donau_module.c

# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).

if MCA_BUILD_orte_schizo_donau_DSO
component_noinst =
component_install = mca_schizo_donau.la
else
component_noinst = libmca_schizo_donau.la
component_install =
endif

mcacomponentdir = $(ortelibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_schizo_donau_la_SOURCES = $(sources)
mca_schizo_donau_la_LDFLAGS = -module -avoid-version
mca_schizo_donau_la_LIBADD = $(top_builddir)/orte/lib@ORTE_LIB_PREFIX@open-rte.la

noinst_LTLIBRARIES = $(component_noinst)
libmca_schizo_donau_la_SOURCES = $(sources)
libmca_schizo_donau_la_LDFLAGS = -module -avoid-version