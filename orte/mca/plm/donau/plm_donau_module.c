/*
 * Copyright (c) 2024      Huawei Technologies Co., Ltd.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 * 
 * These symbols are in a file by themselves to provide nice linker
 * semantics. Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire component just to query their version and parameters.
 */

#include "opal/class/opal_object.h"
#include "orte/mca/plm/plm_types.h"
#include "orte_config.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/oob/base/base.h"
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <signal.h>
#include <stdlib.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include "opal/mca/base/base.h"
#include "opal/mca/installdirs/installdirs.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/opal_environ.h"
#include "opal/util/path.h"
#include "opal/util/basename.h"

#include "orte/constants.h"
#include "orte/types.h"
#include "orte/util/show_help.h"
#include "orte/util/name_fns.h"
#include "orte/util/threads.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/runtime/orte_quit.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/state/state.h"

#include "orte/orted/orted.h"

#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/base/plm_private.h"
#include "plm_donau.h"
#define DONAU_MAX_NODELIST_LENGTH 128
#define DONAU_MAX_NODENAME_LENGTH 1024
/*
 * Local functions
 */
static int plm_donau_init(void);
static int plm_donau_launch_job(orte_job_t *jdata);
static int plm_donau_terminate_orteds(void);
static int plm_donau_signal_job(orte_jobid_t jobid, int32_t signal);
static int plm_donau_finalize(void);

static int plm_donau_start_proc(int argc, char **argv, char **env,
                                char *prefix);

/*
 * Global variable
 */
orte_plm_base_module_1_0_0_t orte_plm_donau_module = {
    plm_donau_init,
    orte_plm_base_set_hnp_name,
    plm_donau_launch_job,
    NULL,
    orte_plm_base_orted_terminate_job,
    plm_donau_terminate_orteds,
    orte_plm_base_orted_kill_local_procs,
    plm_donau_signal_job,
    plm_donau_finalize
};

/*
 * Local variables
 */
static pid_t primary_drun_pid = 0;
static bool primary_pid_set = false;
static void launch_daemons(int fd, short args, void *cbdata);
static char *donau_nodelist_simp(char *nodelist);
/*
 * Init the module
 */
static int plm_donau_init(void)
{
    int rc;
    if (ORTE_SUCCESS != (rc = orte_plm_base_comm_start())) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    /* if we don't want to launch (e.g. someone just wants
     * to test the mappers), then we assign vpids at "launch"
     * so the mapper has something to work with
     */
    if (orte_do_not_launch) {
        orte_plm_globals.daemon_nodes_assigned_at_launch = true;
    } else {
        /* we do NOT assign daemons to nodes at launch - we will
         * determine that mapping when the daemon
         * calls back. This is required because donau does
         * its own mapping of proc-to-node, and we cannot know
         * in advance which daemon will wind up on which node
         */
        orte_plm_globals.daemon_nodes_assigned_at_launch = false;
    }

    /* point to our launch command */
    rc = orte_state.add_job_state(ORTE_JOB_STATE_LAUNCH_DAEMONS,
                                  launch_daemons, ORTE_SYS_PRI);
    if (ORTE_SUCCESS != rc) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    return rc;
}

/* When working in this function, ALWAYS jump to "cleanup" if
 * you encounter an error so that orterun will be woken up and
 * the job can cleanly terminate
 */
static int plm_donau_launch_job(orte_job_t *jdata)
{
    if (ORTE_FLAG_TEST(jdata, ORTE_JOB_FLAG_RESTART)) {
        /* this is a restart situation - skip to the mapping stage */
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_MAP);
    } else {
        /* new job - set it up */
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_INIT);
    }
    return ORTE_SUCCESS;
}

static void launch_daemons(int fd, short args, void *cbdata)
{
    orte_app_context_t *app;
    orte_node_t *node;
    orte_std_cntr_t nnode;
    orte_job_map_t *map;
    char *jobid_string = NULL;
    char *param;
    char **argv = NULL;
    int argc;
    int rc;
    char *tmp;
    char **env = NULL;
    char *nodelist_flat;
    char *nodelist_simp;
    char **nodelist_argv;
    char *name_string;
    char **custom_strings;
    int num_args, i;
    char *cur_prefix;
    int proc_vpid_index;
    bool failed_launch = true;
    orte_job_t *daemons;
    orte_state_caddy_t *state = (orte_state_caddy_t*)cbdata;

    ORTE_ACQUIRE_OBJECT(state);

    OPAL_OUTPUT_VERBOSE((1, orte_plm_base_framework.framework_oupput,
                         "%s plm:donau: LAUNCH DAEMONS CALLED",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* if we are launching debugger daemons, then just go
     * do it - no new daemons will be launched
     */
    if (ORTE_FLAG_TEST(state->jdata, ORTE_JOB_FLAG_DEBUGGER_DAEMON)) {
        state->jdata->state = ORTE_JOB_STATE_DAEMONS_LAUNCHED;
        ORTE_ACTIVATE_JOB_STATE(state->jdata, ORTE_JOB_STATE_DAEMONS_REPORTED);
        OBJ_RELEASE(state);
        return;
    }

    /* start by setting up the virtual machine */
    daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    if (ORTE_SUCCESS != (rc = orte_plm_base_setup_virtual_machine(state->jdata))) {
        ORTE_ERROR_LOG(rc);
        goto cleanup;
    }
    /* if we don't want to launch, then don't attempt to
     * launch the daemons - the user really wants to just
     * look at the proposed process map
     */
    if (orte_do_not_launch) {
        /* set the state to indicate the daemons reported - this
         * will trigger the daemons_reported event and cause the
         * job to move to the following step
         */
        state->jdata->state = ORTE_JOB_STATE_DAEMONS_LAUNCHED;
        ORTE_ACTIVATE_JOB_STATE(state->jdata, ORTE_JOB_STATE_DAEMONS_REPORTED);
        OBJ_RELEASE(state);
        return;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_plm_base_framework.framework_output,
                         "%s plm:donau: launching vm",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));


    /* get the map for the job */
    if (NULL == (map = daemons->map)) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        rc = ORTE_ERR_NOT_FOUND;
        goto cleanup;
    }

    if (0 == map->num_new_daemons) {
        /* set the state to indicate the daemons reported - this
         * will trigger the daemons_reported event and cause the
         * job to move the following step
         */
        OPAL_OUTPUT_VERBOSE((2, orte_plm_base_framework.framework_output,
                             "%s plm:donau: no new daemons to launch",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        state->jdata->state = ORTE_JOB_STATE_DAEMONS_LAUNCHED;
        ORTE_ACTIVATE_JOB_STATE(state->jdata, ORTE_JOB_STATE_DAEMONS_REPORTED);
        OBJ_RELEASE(state);
        return;
    }

    /* need integer value for command line parameter */
    asprintf(&jobid_string, "%lu", (unsigned long) daemons->jobid);
    /*
     * start building argv array
     */
    argv = NULL;
    argc = 0;

    /*
     * DONAU drun OPTIONS
     */

    /* add the drun command */
    opal_argv_append(&argc, &argv, donau_launch_exec);

#if DONAU_CRAY_ENV
    /*
     * If in a DONAU/Cray env. make sure that Cray PMI is not pulled in,
     * neither as a constructor run when orteds start, nor selected
     * when pmix components are registered
     */

    opal_setenv("PMI_NO_PREINITIALIZE", "1", false, &orte_launch_environ);
    opal_setenv("PMI_NO_FORK", "1", false, &orte_launch_environ);
    opal_setenv("PMI_NO_USE_CRAY_PMI", "1", false, &orte_launch_environ);
#endif

    /* Append user defined arguments to drun */
    if (NULL != mca_plm_donau_component.custom_args) {
        custom_strings = opal_argv_split(mca_plm_donau_component.custom_args, ' ');
        num_args = opal_argv_count(custom_strings);
        for (i = 0; i < num_args; ++i) {
            opal_argv_append(&argc, &argv, custom_strings[i]);
        }
        opal_argv_free(custom_strings);
    }
    /* create nodelist */
    nodelist_argv = NULL;

    for (nnode = 0; nnode < map->nodes->size; nnode++) {
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(map->nodes, nnode))){
            continue;
        }
        /* if the daemon already exists on this node, then
         * don't include it
         */
        if (ORTE_FLAG_TEST(node, ORTE_NODE_FLAG_DAEMON_LAUNCHED)) {
            continue;
        }

        /* otherwise, add it to the list of nodes upon which
         * we need to launch a daemon
         */
        opal_argv_append_nosize(&nodelist_argv, node->name);
    }
    if (0 == opal_argv_count(nodelist_argv)) {
        orte_show_help("help-plm-donau.txt", "no-hosts-in-list", true);
        rc = ORTE_ERR_FAILED_TO_START;
        goto cleanup;
    }
    nodelist_flat = opal_argv_join(nodelist_argv, ',');
    opal_argv_free(nodelist_argv);

    /* simplify nodelist for donau */
    nodelist_simp = NULL;
    nodelist_simp = donau_nodelist_simp(nodelist_flat);

    asprintf(&tmp, "-nl");
    opal_argv_append(&argc, &argv, tmp);
    asprintf(&tmp, "%s", nodelist_flat);
    opal_argv_append(&argc, &argv, tmp);
    asprintf(&tmp, "-rpn");
    opal_argv_append(&argc, &argv, tmp);
    asprintf(&tmp, "1");
    opal_argv_append(&argc, &argv, tmp);
    free(tmp);

    OPAL_OUTPUT_VERBOSE((2, orte_plm_base_framework.framework_output,
                             "%s plm:donau: launching on nodes %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), nodelist_flat));
    free(nodelist_simp);
    /*
     * ORTED OPTIONS
     */

    /* add the daemon command (as specified by user) */
    orte_plm_base_setup_orted_cmd(&argc, &argv);
    /* add basic orted command line options, including debug flags */
    orte_plm_base_orted_append_basic_args(&argc, &argv,
                                          "donau", &proc_vpid_index);
    /* tell the new daemons the base of the name list so they can compute
     * their own name on the other end
     */
    rc = orte_util_convert_vpid_to_string(&name_string, map->daemon_vpid_start);
    if (ORTE_SUCCESS != rc) {
        opal_output(0, "plm_donau: unable to get daemon vpid as string");
        goto cleanup;
    }

    free(argv[proc_vpid_index]);
    argv[proc_vpid_index] = strdup(name_string);
    free(name_string);

    char *param1;
    orte_oob_base_get_addr(&param1);
    opal_argv_append(&argc, &argv, "-"OPAL_MCA_CMD_LINE_ID);
    opal_argv_append(&argc, &argv, "orte_parent_uri");
    opal_argv_append(&argc, &argv, param1);
    /* Copy the prefix-directory specified in the
     * corresponding add_context. If there are multiple,
     * different prefix's in the app context, complain (i.e., only
     * allow one --prefix option for the entire donau run -- we
     * don't support different --prefix'es for different nodes in
     * the DONAU plm)
     */
    cur_prefix = NULL;
    for (nnode = 0; nnode < state->jdata->apps->size; nnode++) {
        char *app_prefix_dir = NULL;
        if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(state->jdata->apps, nnode))) {
            continue;
        }
        app_prefix_dir = NULL;
        orte_get_attribute(&app->attributes, ORTE_APP_PREFIX_DIR, (void**)&app_prefix_dir, OPAL_STRING);
        /* Check for already set cur_prefix_dir -- if different,
           complain */
        if (NULL != app_prefix_dir) {
            if (NULL != app_prefix_dir &&
                0 != strcmp(cur_prefix, app_prefix_dir)) {
                orte_show_help("help-plm-donau.txt", "multiple-prefixes",
                            true, cur_prefix, app_prefix_dir);
                goto cleanup;
            }

            /* If not yet set, copy it; if set, then it's the
             * same way
             */
            if (NULL == cur_prefix) {
                cur_prefix = strdup(app_prefix_dir);
                OPAL_OUTPUT_VERBOSE((1, orte_plm_base_framework.framework_output,
                                     "%s plm:donau: Set prefix:%s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     cur_prefix));
            }
            free(app_prefix_dir);
        }
    }

    /* protect the args in case someone has a script wrapper around drun */
    mca_base_cmd_line_wrap_args(argv);

    /* setup environment */
    env = opal_argv_copy(orte_launch_environ);

    if (0 < opal_output_get_verbosity(orte_plm_base_framework.framework_output)) {
        param = opal_argv_join(argv, ' ');
        opal_output(orte_plm_base_framework.framework_output,
                    "%s plm:donau: final top-level argv:\n\t%s",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    (NULL == param) ? "NULL" : param);
        if (NULL != param) {
            free(param);
        }
    }

    /* exec the daemon(s) */
    if (ORTE_SUCCESS != (rc = plm_donau_start_proc(argc, argv, env, cur_prefix))) {
        ORTE_ERROR_LOG(rc);
        goto cleanup;
    }

    /* indicate that the daemons for this job were launched */
    state->jdata->state = ORTE_JOB_STATE_DAEMONS_LAUNCHED;
    daemons->state = ORTE_JOB_STATE_DAEMONS_LAUNCHED;

    /* flag that launch was successful, so far as we currently know */
    failed_launch = false;

cleanup:
    if (NULL != argv) {
        opal_argv_free(argv);
    }
    if (NULL != env) {
        opal_argv_free(env);
    }
    if (NULL != jobid_string) {
        opal_argv_free(jobid_string);
    }
    /* cleanup the caddy */
    OBJ_RELEASE(state);

    /* check for failed launch - if so, force terminate */
    if (failed_launch) {
        ORTE_FORCED_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
    }
}

/*
 * Terminate the orteds for a given job
 */
static int plm_donau_terminate_orteds(void)
{
    int rc = ORTE_SUCCESS;
    orte_job_t *jdata;

    /* check to see if the primary pid is set. If not, this indicates
     * that we never launched any additional daemons, so we cannot
     * not wait for a waitpid to fire and tell us it's okay to
     * exit. Instead, we simply trigger an exit for ourselves.
     */
    if (primary_pid_set) {
        if (ORTE_SUCCESS != (rc = orte_plm_base_orted_exit(ORTE_DAEMON_EXIT_CMD))) {
            ORTE_ERROR_LOG(rc);
        }
    } else {
        OPAL_OUTPUT_VERBOSE((1, orte_plm_base_framework.framework_output,
                             "%s plm:donau: primary daemons complete!",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        jdata = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
        /* need to set the $terminated value to avoid an incorrect error msg */
        jdata->num_terminated = jdata->num_procs;
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_DAEMONS_TERMINATED);
    }

    return rc;
}

/*
 * Signal all the processes in the child drun by sending the signal directly to it
 */
static int plm_donau_signal_job(orte_jobid_t jobid, int32_t signal)
{
    int rc = ORTE_SUCCESS;

    /* order them to pass this signal to their local procs */
    if (ORTE_SUCCESS != (rc = orte_plm_base_orted_signal_local_procs(jobid, signal))) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

static int plm_donau_finalize(void)
{
    int rc;

    /* cleanup any pending recvs */
    if (ORTE_SUCCESS != (rc = orte_plm_base_comm_stop())) {
        ORTE_ERROR_LOG(rc);
    }

    return ORTE_SUCCESS;
}

static void drun_wait_cb(int sd, short fd, void *cbdata)
{
    orte_wait_tracker_t *t2 = (orte_wait_tracker_t*)cbdata;
    orte_proc_t *proc = t2->child;
    orte_job_t *jdata;

    jdata = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);

    /* abort only if the status returned is non-zero - i.e., if
     * the orteds exited with an error
     */
    if (0 != proc->exit_code) {
        /* an orted must have died unexpectedly - report
         * that the daemon has failed so we exit
         */
        OPAL_OUTPUT_VERBOSE((1, orte_plm_base_framework.framework_output,
                             "%s plm:donau: drun returned non-zero exit status (%d) from launching the per-node daemon",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             proc->exit_code));
        ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_ABORTED);
    } else {
        /* otherwise, check to see if this is the primary pid */
        if (primary_drun_pid == proc->pid) {
            /* in this case, we just want to fire the proper trigger so
             * mpirun can exit
             */
            OPAL_OUTPUT_VERBOSE((1, orte_plm_base_framework.framework_output,
                                 "%s plm:donau: primary daemons complete!",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            /* need to set the #terminated value to avoid an incorrect error msg */
            jdata->num_terminated = jdata->num_procs;
            ORTE_ACTIVATE_JOB_STATE(jdata, ORTE_JOB_STATE_DAEMONS_TERMINATED);
        }
    }

    /* done with this dummy */
    OBJ_RELEASE(t2);

}

static int plm_donau_start_proc(int argc, char **argv, char **env,
                                char *prefix)
{
    int fd;
    int drun_pid;
    char *exec_argv = opal_path_findv(argv[0], 0, env, NULL);
    orte_proc_t *dummy;

    if (NULL == exec_argv) {
        orte_show_help("help-plm-donau.txt", "no-drun", true);
        return ORTE_ERR_SILENT;
    }

    drun_pid = fork();

    if (-1 == drun_pid) {
        ORTE_ERROR_LOG(ORTE_ERR_SYS_LIMITS_CHILDREN);
        free(exec_argv);
        return ORTE_ERR_SYS_LIMITS_CHILDREN;
    }
    /* if this is the primary launch - i.e., not a comm_spawn of a
     * child job - then save the pid
     */
    if (0 < drun_pid && !primary_pid_set) {
        primary_drun_pid = drun_pid;
        primary_pid_set = true;
    }

    /* setup a dummy proc object to track the drun */
    dummy = OBJ_NEW(orte_proc_t);
    dummy->pid = drun_pid;
    /* be sure to mark it as alive so we don't instantly fire */
    ORTE_FLAG_SET(dummy, ORTE_PROC_FLAG_ALIVE);
    /* setup the waitpid so we can find out if drun succeeds! */
    orte_wait_cb(dummy, drun_wait_cb, orte_event_base, NULL);


    if (0 == drun_pid) { /* child */
        char *bin_base = NULL;
        char *lib_base = NULL;

        /* Figure out the basenames for the libdir and bindir.  There
         * is a lengthy comment about this in plm_rsh_module.c
         * explaining all the rationale for how / why we're doing
         * this.
         */
        lib_base = opal_basename(opal_install_dirs.libdir);
        bin_base = opal_basename(opal_install_dirs.bindir);

        /* If we have a prefix, then modify the PATH and
         * LD_LIBRARY_PATH environment variables.
         */
        if (NULL != prefix) {
            char *oldenv, *newenv;

            /* Reset PATH */
            oldenv = getenv("PATH");
            if (NULL != oldenv) {
                asprintf(&newenv, "%s/%s:%s", prefix, bin_base, oldenv);
            } else {
                asprintf(&newenv, "%s/%s", prefix, bin_base);
            }
            opal_setenv("PATH", newenv, true, &env);
            OPAL_OUTPUT_VERBOSE((1, orte_plm_base_framework.framework_output,
                                 "%s plm:donau: reset PATH: %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 newenv));
            free(newenv);

            /* Reset LD_LIBRARY_PATH */
            oldenv = getenv("LD_LIBRARY_PATH");
            if (NULL != oldenv) {
                asprintf(&newenv, "%s/%s:%s", prefix, lib_base, oldenv);
            } else {
                asprintf(&newenv, "%s/%s", prefix, lib_base);
            }
            opal_setenv("LD_LIBRARY_PATH", newenv, true, &env);
            OPAL_OUTPUT_VERBOSE((1, orte_plm_base_framework.framework_output,
                                 "%s plm:donau: reset LD_LIBRARY_PATH: %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 newenv));
            free(newenv);
        }

        fd = open("/dev/null", O_CREAT|O_RDWR|O_TRUNC, 0666);
        if (fd >= 0) {
            dup2(fd, 0);
            /* When not in debug mode and --debug-daemons was not passed,
             * tie sedout/stderr to dev null so we don't see messages from orted
             * EXCEPT if the user has requested that we leave sessions attached
             */
            if (0 > opal_output_get_verbosity(orte_plm_base_framework.framework_output) &&
                !orte_debug_daemons_flag && !orte_leave_session_attached) {
                dup2(fd, 1);
                dup2(fd, 2);
            }

            /* Don't leave the extra fd to /dev/null open */
            if (fd > 2) {
                close(fd);
            }
        }

        /* get the drun process out of orterun's process group so that
           singnals sent from the shell (like those resulting from
           cntl-c) don't get sent to drun */
        setpgid(0, 0);
        execve(exec_argv, argv, env);

        opal_output(0, "plm:donau:start_proc: exec failed");
        /* don't return - need to exit - returning would be bad -
         * we're not in the calling process anymore 
         */
        exit(1);
    } else { /* parent */
        /* just in case, make sure that the drun process is not in our
         * process group any more. Stevens says always do this on both
         * sides of the fork... */
        setpgid(drun_pid, drun_pid);

        free(exec_argv);
    }

    return ORTE_SUCCESS;
}

static char *donau_nodelist_simp(char *nodelist)
{
    char* result = (char*)malloc(DONAU_MAX_NODELIST_LENGTH * sizeof(char));
    result[0] = '\0';
    char *nodelist_bak = strdup(nodelsit);
    char* token = strtok(nodelist);
    char prefix[DONAU_MAX_NODENAME_LENGTH];
    int start = -1, end = -1;
    while (token != NULL)
    {
        char temp_prefix[DONAU_MAX_NODENAME_LENGTH];
        int num;
        sscanf(token, "%[^0-9]%d", temp_prefix, &num);
        if (strcmp(temp_prefix, prefix) != 0) {
            if (start != -1) {
                if (start == end) sprintf(result + strlen(result), "%d] ", start);
                else sprintf(result + strlen(result), "%d-%d] ", start, end);
            } 
            strcpy(prefix, temp_prefix);
            sprintf(result + strlen(result), "%s[", prefix);
            start = end = num;
        } else {
                if (num == end + 1) end = num;
                else {
                    if (start == end) sprintf(result + strlen(result), "%d,", start);
                    else sprintf(result + strlen(result), "%d-%d", start, end);
                    start = end = num;
                }
            }
        token = strtok(NULL, ",");
    }
    if (start == end) sprintf(result + strlen(result), "%d]\n", start);
    else sprintf(result + strlen(result), "%d-%d]\n", start, end);

    return result;
}