import argparse
import copy
import logging
import os
import pwd
import random
import re
import sys
import time

from jobman import sql
from jobman.channel import Channel
from jobman.tools import expand, resolve

from distributed_jobman import get_db_string
import observer
from plot import plot
import schedulers.jobs as job_scheduler
import schedulers.experiments as experiment_scheduler
from utils import bold, query_yes_no, load_json, ChangeDir

logger = logging.getLogger("main")
LOGGING_FORMAT = "%(asctime)-15s %(levelname)s:%(name)s:%(filename)-20s %(message)s"

LAUNCH = "launch"
RUN = "run"
MONITOR = "monitor"
LIST = "list"
SET = "set"
RESET = "reset"
REMOVE = "remove"
PLOT = "plot"


def get_options(argv):

    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--cluster", default=None, help="""
        WRITEME""")

    parser.add_argument("-v", "--verbose", action="store_true", help="""
        WRITEME""")

    subparsers = parser.add_subparsers(dest='command')
    launch_parser = subparsers.add_parser(LAUNCH)
    run_parser = subparsers.add_parser(RUN)
    monitor_parser = subparsers.add_parser(MONITOR)
    list_parser = subparsers.add_parser(LIST)
    set_parser = subparsers.add_parser(SET)
    reset_parser = subparsers.add_parser(RESET)
    remove_parser = subparsers.add_parser(REMOVE)
    plot_parser = subparsers.add_parser(PLOT)

    for subparser in [launch_parser, monitor_parser, list_parser]:
        subparser.add_argument("-c", "--cluster", default=None, help="""
            WRITEME""")

    # Launch parser arguments

    launch_parser.add_argument("-l", "--limit", type=int, default=0, help="""
        Limit the number of jobs that can be launched""")
    launch_parser.add_argument("-e", "--experiment", help="""
        Only launch the given experiment name""")

    # Run parser arguments

    run_parser.add_argument("main_function_path", help="""
        WRITEME""")
    run_parser.add_argument("experiment_config", help="""
        WRITEME""")
    run_parser.add_argument("job_config", help="""
        WRITEME""")
    run_parser.add_argument("-f", "--force", action="store_true", help="""
        WRITEME""")

    # Set parser arguments

    set_parser.add_argument("experiment_name", help="""
        WRITEME""")
    set_parser.add_argument(
        "job_status", choices=["pending", "running", "completed", "broken"], help="""
            WRITEME""")
    set_parser.add_argument(
        "new_status", choices=["pending", "running", "completed"], help="""
            WRITEME""")

    # Reset parser arguments

    reset_parser.add_argument("experiment_name", help="""
        WRITEME""")
    reset_parser.add_argument(
        "job_status", choices=["running", "completed", "broken"], help="""
            WRITEME""")

    # Remove parser arguments

    remove_parser.add_argument("name", help="""
        WRITEME""")

    # Plot parser arguments

    plot_parser.add_argument("experiment_name", help="""
        WRITEME""")

    options = parser.parse_args(argv)

    if options.cluster:
        print "------------------------%s" % ("-" * len(options.cluster))
        print "Experiments for cluster %s" % bold(options.cluster)
        print "------------------------%s\n" % ("-" * len(options.cluster))
    else:
        print "-----------------------------"
        print "Experiments for %s clusters" % bold("all")
        print "-----------------------------\n"

    return options


def _format_ids(jobs, show_maximum=20):
    ids = [str(job["id"]) for job in jobs]
    ids_string = ", ".join(sorted(ids)[:show_maximum])
    if len(ids) > show_maximum:
        ids_string += ", ..."

    return ids_string


def list_experiments(cluster):
    experiments = experiment_scheduler.load_experiments(cluster)

    if len(experiments) == 0:
        print "No experiments in database %s" % get_db_string("experiments")

    for experiment in experiments:
        print "Name:", bold(experiment["name"])
        print "  Id: %d" % experiment["jobman"]["id"]
        for key in ["table", "gpu"]:
            print "      %s = %s" % (key, str(experiment[key]))

        for name, cluster in experiment["clusters"].iteritems():
            print "      %s:" % name
            for key, value in sorted(cluster.iteritems()):
                print "          %s = %s" % (key, value)

        list_jobs(experiment["table"])
        print


def list_jobs(table_name):
    jobs = job_scheduler.load_jobs(table_name)
    waiting_jobs = []
    running_jobs = []
    completed_jobs = []
    broken_jobs = []
    for job in jobs:
        if job_scheduler.is_pending(job):
            waiting_jobs.append(job)
        elif job_scheduler.is_running(job):
            running_jobs.append(job)
        elif job_scheduler.is_completed(job):
            completed_jobs.append(job)
        elif job_scheduler.is_broken(job):
            broken_jobs.append(job)

    print ("      # of jobs in total = % 3d" % len(jobs))
    print ("                 waiting = % 3d    {%s}" %
           (len(waiting_jobs), _format_ids(waiting_jobs)))
    print ("                 running = % 3d    {%s}" %
           (len(running_jobs), _format_ids(running_jobs)))
    print ("               completed = % 3d    {%s}" %
           (len(completed_jobs), _format_ids(completed_jobs)))
    print ("                  broken = % 3d    {%s}" %
           (len(broken_jobs), _format_ids(broken_jobs)))


def monitor(cluster):
    if cluster is None:
        raise ValueError("cluster must be specified for monitoring")

    experiments = experiment_scheduler.load_experiments(cluster)

    if len(experiments) == 0:
        print "No experiments in database %s" % get_db_string("experiments")
        return

    for experiment in experiments:
        nb_of_jobs_to_launch = monitor_single_experiment(cluster, experiment)

        print "would submit %d new jobs\n" % nb_of_jobs_to_launch


def monitor_single_experiment(cluster, experiment):
    username = pwd.getpwuid(os.getuid()).pw_name

    print "Verifying jobs for experiment \"%s\"" % experiment["name"]
    nb_of_waiting_jobs = observer.count_pending_jobs(experiment["table"])
    nb_of_user_submitted_jobs = observer.count_submitted_jobs(username)

    print "%d jobs waiting" % nb_of_waiting_jobs
    print ("%d jobs submitted (from all experiments)" %
           nb_of_user_submitted_jobs)

    max_queued_jobs = max(experiment["clusters"][cluster]["max_running"] -
                          nb_of_user_submitted_jobs, 0)

    nb_of_jobs_to_launch = min(nb_of_waiting_jobs, max_queued_jobs)

    if experiment["clusters"][cluster]["max_queued"] > 0:
        nb_of_total_submitted_jobs = observer.count_submitted_jobs()
        still_free = (experiment["clusters"][cluster]["max_queued"] -
                      nb_of_total_submitted_jobs)
        print "%s cores are free" % still_free
        nb_of_jobs_to_launch = min(still_free, nb_of_jobs_to_launch)

    return nb_of_jobs_to_launch


def launch(cluster, limit, experiment):
    if cluster is None:
        raise ValueError("cluster must be specified for launch option")

    if experiment:
        filter_eq_dct = dict(name=experiment)
    else:
        filter_eq_dct = None

    experiments = experiment_scheduler.load_experiments(
        cluster, filter_eq_dct=filter_eq_dct)

    random.shuffle(experiments)

    if len(experiments) == 0:
        print "No experiments in database %s" % get_db_string("experiments")
        return

    for experiment in experiments:
        nb_of_jobs_to_launch = monitor_single_experiment(cluster, experiment)

        if nb_of_jobs_to_launch > 0:
            if limit and nb_of_jobs_to_launch > limit:
                print "would submit %d new jobs" % nb_of_jobs_to_launch
                nb_of_jobs_to_launch = limit
            print "submitting %d new jobs" % nb_of_jobs_to_launch
            job_scheduler.submit_job(cluster, experiment, nb_of_jobs_to_launch)
        else:
            print "no job to launch"

        print "\n"
        time.sleep(15)


function_path_re = re.compile('\.py$')


def run(function_path, experiment_config_file, job_config_file, force):

    job_dir = os.path.dirname(job_config_file)
    experiment_config = load_json(os.path.join(experiment_config_file))
    state = load_json(job_config_file)

    with ChangeDir(job_dir):

        experiment = experiment_scheduler.save_experiment(
            name=experiment_config["experiment_name"],
            table_name=experiment_config["experiment_name"] + "_jobs",
            clusters=experiment_config["clusters"],
            duree="", mem="", env="", gpu="")

        table_name = experiment["table"]

        channel = Channel()

        state["jobman"] = dict(status=channel.START)
        state_to_hash = copy.copy(state)
        jobs = job_scheduler.load_jobs(table_name, hash_of=state_to_hash)
        state_to_hash["jobman"] = dict(status=channel.RUNNING)
        jobs += job_scheduler.load_jobs(table_name, hash_of=state_to_hash)
        if len(jobs) > 0:
            logger.warning("Job already registered, loading from database")
            state = jobs[0]

        if state["jobman"]["status"] != channel.START:
            if not force:
                raise RuntimeError("Job (%d) is not available" % state["id"])

            logging.warning("Job (%d) is not available. Forcing it to run" %
                            state["id"])

        state["jobman"]["status"] = channel.RUNNING

        state = job_scheduler.save_job(table_name, state)

        resolve(function_path_re.sub('', function_path)).jobman_main(state, channel)

        job_scheduler.save_job(experiment["table"], state)


def set_jobs(name, status, new_status):
    experiments = experiment_scheduler.load_experiments(
        cluster=None, filter_eq_dct=dict(name=name))

    if len(experiments) == 0:
        print "No experiments in database %s" % get_db_string("experiments")
        return

    experiment = experiments[0]

    if status == "pending":
        jobs = job_scheduler.load_pending_jobs(experiment["table"])
    elif status == "broken":
        jobs = job_scheduler.load_broken_jobs(experiment["table"])
    elif status == "completed":
        jobs = job_scheduler.load_completed_jobs(experiment["table"])
    elif status == "running":
        jobs = job_scheduler.load_running_jobs(experiment["table"])

    if new_status == "pending":
        sql_new_status = sql.START
        new_status = "pending"
    elif new_status == "running":
        sql_new_status = sql.RUNNING
    elif new_status == "completed":
        sql_new_status = sql.COMPLETE

    print "Setting %s jobs to %s status..." % (status, new_status)
    job_scheduler.update_jobs(experiment["table"], jobs,
                              expand({sql.STATUS: sql_new_status,
                                      'proc_status': new_status}))


def reset_jobs(name, status):
    set_jobs(name, status, "pending")


def remove_experiment(name):
    experiments = experiment_scheduler.load_experiments(
        cluster=None, filter_eq_dct=dict(name=name))

    if len(experiments) == 0:
        print "No experiments in database %s" % get_db_string("experiments")
        return

    experiment = experiments[0]
    table_name = experiment["table"]
    if query_yes_no("Do you really want to delete experiment %s?" % bold(name)):
        print "Deleting %s..." % name
        experiment_scheduler.delete_experiments([experiment])
    if query_yes_no("Do you want to delete corresponding jobs?"):
        jobs = job_scheduler.load_jobs(table_name)
        print "Deleting %d jobs..." % len(jobs)
        job_scheduler.delete_jobs(table_name, jobs)
    # if query_yes_no("Do you want to delete corresponding files?"):
    #    pass


def plot_experiment(experiment_name):
    experiments = experiment_scheduler.load_experiments(
        cluster=None, filter_eq_dct=dict(name=experiment_name))

    if len(experiments) == 0:
        print "No experiments in database %s" % get_db_string("experiments")

    experiment = experiments[0]

    jobs = job_scheduler.load_jobs(experiment['table'])
    return plot(jobs)


def main(argv):
    options = get_options(argv)

    if options.verbose:
        logging.basicConfig(level=logging.DEBUG, format=LOGGING_FORMAT)
    else:
        logging.basicConfig(level=logging.WARNING, format=LOGGING_FORMAT)

    if options.command == LAUNCH:
        launch(options.cluster, options.limit, options.experiment)
    if options.command == RUN:
        run(options.main_function_path, options.experiment_config,
            options.job_config, options.force)
    elif options.command == MONITOR:
        monitor(options.cluster)
    elif options.command == LIST:
        list_experiments(options.cluster)
    elif options.command == RESET:
        reset_jobs(options.experiment_name,
                   options.job_status)
    elif options.command == REMOVE:
        remove_experiment(options.name)
    elif options.command == PLOT:
        plot_experiment(options.experiment_name)


if __name__ == "__main__":
    main(sys.argv[1:])
