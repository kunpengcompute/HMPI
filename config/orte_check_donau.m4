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

# ORTE_CHECK_DONAU(prefix, [action-if-found], [action-if-not-found])
# --------------------------------------------------------
AC_DEFUN([ORTE_CHECK_DONAU],[
    if test -z "$orte_check_donau_happy" ; then
    AC_ARG_WITH([donau],
            [AC_HELP_STRING([--with-donau],
                            [Build DONAU scheduler component (default: yes)])])

    if test "$with_donau" = "no" ; then
            orte_check_donau_happy="no"
    elif test "$with_donau" = "" ; then
            # unless user asked, only build donau component on linux, AIX,
            # and OS X systems (these are the platforms that DONAU
            # supports)
            case $host in
        *-linux*|*-aix*|*-apple-darwin*)
                    orte_check_donau_happy="yes"
                    ;;
        *)
                    AC_MSG_CHECKING([for DONAU drun in PATH])
            OPAL_WHICH([drun], [ORTE_CHECK_DONAU_DRUN])
                    if test "$ORTE_CHECK_DONAU_DRUN" = ""; then
            orte_check_donau_happy="no"
                    else
            orte_check_donau_happy="yes"
                    fi
                    AC_MSG_RESULT([$orte_check_donau_happy])
                    ;;
            esac
        else
            orte_check_donau_happy="yes"
        fi

        AS_IF([test "$orte_check_donau_happy" = "yes"],
              [AC_CHECK_FUNC([fork],
                             [orte_check_donau_happy="yes"],
                             [orte_check_donau_happy="no"])])

        AS_IF([test "$orte_check_donau_happy" = "yes"],
              [AC_CHECK_FUNC([execve],
                             [orte_check_donau_happy="yes"],
                             [orte_check_donau_happy="no"])])

        AS_IF([test "$orte_check_donau_happy" = "yes"],
              [AC_CHECK_FUNC([setpgid],
                             [orte_check_donau_happy="yes"],
                             [orte_check_donau_happy="no"])])
        OPAL_SUMMARY_ADD([[Resource Managers]],[[Donau]],[$1],[$orte_check_donau_happy])
    fi

    AS_IF([test "$orte_check_donau_happy" = "yes"],
          [$2],
          [$3])
])