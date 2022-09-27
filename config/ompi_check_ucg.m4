dnl -*- shell-script -*-
dnl
dnl Copyright (c) 2022      Huawei Technologies Co., Ltd. All rights reserved.
dnl
dnl $COPYRIGHT$
dnl
dnl Additional copyrights may follow
dnl
dnl $HEADER$
dnl

# OMPI_CHECK_UCG(prefix, [action-if-found], [action-if-not-found])
# --------------------------------------------------------
# check if ucg support can be found.  sets prefix_{CPPFLAGS,
# LDFLAGS, LIBS} as needed and runs action-if-found if there is
# support, otherwise executes action-if-not-found
AC_DEFUN([OMPI_CHECK_UCG],[
    OPAL_VAR_SCOPE_PUSH([ompi_check_ucg_dir ompi_check_ucg_libs ompi_check_ucg_happy CPPFLAGS_save LDFLAGS_save LIBS_save])

    AC_ARG_WITH([ucg],
                [AS_HELP_STRING([--with-ucg(=DIR)],
                                [Build UCG (Unified Collective Group)])])

    AS_IF([test "$with_ucg" != "no"],
          [ompi_check_ucg_libs=ucg
           AS_IF([test ! -z "$with_ucg" && test "$with_ucg" != "yes"],
                 [ompi_check_ucg_dir=$with_ucg])

           CPPFLAGS_save=$CPPFLAGS
           LDFLAGS_save=$LDFLAGS
           LIBS_save=$LIBS

           OPAL_LOG_MSG([$1_CPPFLAGS : $$1_CPPFLAGS], 1)
           OPAL_LOG_MSG([$1_LDFLAGS  : $$1_LDFLAGS], 1)
           OPAL_LOG_MSG([$1_LIBS     : $$1_LIBS], 1)

           OPAL_CHECK_PACKAGE([$1],
                              [ucg/api/ucg.h],
                              [$ompi_check_ucg_libs],
                              [ucg_cleanup],
                              [],
                              [$ompi_check_ucg_dir],
                              [],
                              [ompi_check_ucg_happy="yes"],
                              [ompi_check_ucg_happy="no"])

           AS_IF([test "$ompi_check_ucg_happy" = "yes"],
                 [
                     CPPFLAGS=$coll_ucg_CPPFLAGS
                     LDFLAGS=$coll_ucg_LDFLAGS
                     LIBS=$coll_ucg_LIBS
                 ],
                 [])

           CPPFLAGS=$CPPFLAGS_save
           LDFLAGS=$LDFLAGS_save
           LIBS=$LIBS_save],
           [ompi_check_ucg_happy=no])

    AS_IF([test "$ompi_check_ucg_happy" = "yes" && test "$enable_progress_threads" = "yes"],
          [AC_MSG_WARN([ucg driver does not currently support progress threads.  Disabling UCG.])
           ompi_check_ucg_happy="no"])

    AS_IF([test "$ompi_check_ucg_happy" = "yes"],
          [$2],
          [AS_IF([test ! -z "$with_ucg" && test "$with_ucg" != "no"],
                 [AC_MSG_ERROR([UCG support requested but not found.  Aborting])])
           $3])

    OPAL_VAR_SCOPE_POP
])