#
# Copyright (c) 2024      Huawei Technologies Co., Ltd.
#                         All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

AM_CPPFLAGS = $(ras_donau_CPPFLAGS)

dist_ortedata_DATA = help-ras-donau.txt
sources = \
        ras_donau.h \
        ras_donau_component.c \
        ras_donau_module.c


# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).

if MCA_BUILD_orte_ras_donau_DSO
lib =
lib_sources =
component = mca_ras_donau.la
component_sources = $(sources)
else
lib = libmca_ras_donau.la
lib_sources = $(sources)
component =
component_sources =
endif

mcacomponentdir = $(ortelibdir)
mcacomponent_LTLIBRARIES = $(component)
mca_ras_donau_la_SOURCES = $(component_sources)
mca_ras_donau_la_LDFLAGS = -module -avoid-version $(ras_donau_LDFLAGS)
mca_ras_donau_la_LIBADD = $(top_builddir)/orte/lib@ORTE_LIB_PREFIX@open-rte.la \
        $(ras_donau_LIBS)

noinst_LTLIBRARIES = $(lib)
libmca_ras_donau_la_SOURCES = $(lib_sources)
libmca_ras_donau_la_LDFLAGS = -module -avoid-version $(ras_donau_LDFLAGS)
libmca_ras_donau_la_LIBADD = $(ras_donau_LIBS)