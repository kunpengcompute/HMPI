# -*- shell-script -*-
#
# Copyright (c) 2024      Huawei Technologies Co., Ltd.
#                         All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

# MCA_schizo_donau_CONFIG([action-if-found], [action-if-not-found])
# -----------------------------------------------------------
AC_DEFUN([MCA_orte_schizo_donau_CONFIG],[
    AC_CONFIG_FILES([orte/mca/schizo/donau/Makefile])

    ORTE_CHECK_DONAU([schizo_donau], [schizo_donau_good=1], [schizo_donau_good=0])

    # if check worked, set wrapper flags if so.
    # Evaluate succeed / fail
    AS_IF([test "$schizo_donau_good" = "1"],
          [$1],
          [$2])

    # set build flags to use in makefile
    AC_SUBST([schizo_donau_CPPFLAGS])
    AC_SUBST([schizo_donau_LDFLAGS])
    AC_SUBST([schizo_donau_LIBS])
])dnl