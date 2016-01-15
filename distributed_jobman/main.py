import argparse
import logging
import os
import pwd
import sys

from jobman import sql

from distributed_jobman import get_db_string
import observer
import schedulers.jobs as job_scheduler
import schedulers.experiments as experiment_scheduler

from utils import bold, query_yes_no

LOGGING_FORMAT = "%(asctime)-15s %(levelname)s:%(name)s:%(filename)-20s %(message)s"

LAUNCH = "launch"
LAUNCH_LOCAL = "launch-local"
MONITOR = "monitor"
LIST = "list"
RESET = "reset"
REMOVE = "remove"


def get_options(argv):

    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--cluster", default=None, help="""
        WRITEME""")

    parser.add_argument("-v", "--verbose", action="store_true", help="""
        WRITEME""")

    subparsers = parser.add_subparsers(dest='command')
    launch_parser = subparsers.add_parser(LAUNCH)
    launch_local_parser = subparsers.add_parser(LAUNCH_LOCAL)
    monitor_parser = subparsers.add_parser(MONITOR)
    list_parser = subparsers.add_parser(LIST)
    reset_parser = subparsers.add_parser(RESET)
    remove_parser = subparsers.add_parser(REMOVE)

    for subparser in [launch_parser, monitor_parser]:
        subparser.add_argument("-c", "--cluster", default=None, help="""
            WRITEME""")

    launch_local_parser.add_argument("experiment_name", help="""
        WRITEME""")
    launch_local_parser.add_argument("root", default=".", nargs="?", help="""
        WRITEME""")
    launch_local_parser.add_argument("-n", default=1, type=int, help="""
        WRITEME""")

    reset_parser.add_argument("experiment_name", help="""
        WRITEME""")
    reset_parser.add_argument(
        "job_status", choices=["running", "completed", "broken"], help="""
            WRITEME""")
    remove_parser.add_argument("name", help="""
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


def list_experiments(cluster):
    experiments = experiment_scheduler.load_experiments(cluster)

    if len(experiments) == 0:
        print "No experiments in database %s" % get_db_string("experiments")

    for experiment in experiments:
        print "Name:", bold(experiment["name"])
        print "  Id: %d" % experiment[sql.JOBID]
        for key in ["table", "gpu"]:
            print "      %s = %s" % (key, str(experiment[key]))

        for name, cluster in experiment["clusters"].iteritems():
            print "      %s:" % name
            for key, value in sorted(cluster.iteritems()):
                print "          %s = %s" % (key, value)

        table_name = experiment["table"]

        print "      # of jobs in total = %d" % len(job_scheduler.load_jobs(table_name))
        print ("                 waiting = %d" %
               observer.count_pending_jobs(table_name))
        print ("                 running = %d" %
               observer.count_running_jobs(table_name))
        print ("               completed = %d" %
               observer.count_completed_jobs(table_name))
        print ("                  broken = %d" %
               observer.count_broken_jobs(table_name))
        print


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


def launch(cluster):
    if cluster is None:
        raise ValueError("cluster must be specified for launch option")

    experiments = experiment_scheduler.load_experiments(cluster)

    if len(experiments) == 0:
        print "No experiments in database %s" % get_db_string("experiments")
        return

    for experiment in experiments:
        nb_of_jobs_to_launch = monitor_single_experiment(cluster, experiment)

        if nb_of_jobs_to_launch > 0:
            print "submitting %d new jobs" % nb_of_jobs_to_launch
            job_scheduler.submit_job(cluster, experiment, nb_of_jobs_to_launch)
        else:
            print "no job to launch"

        print "\n"


def launch_local(name, nb_of_jobs_to_launch, root):
    experiments = experiment_scheduler.load_experiments(
        cluster=None, filter_eq_dct=dict(name=name))

    if len(experiments) == 0:
        print "No experiments in database %s" % get_db_string("experiments")
        return

    experiment = experiments[0]

    job_scheduler.submit_local_job(experiment, nb_of_jobs_to_launch, root)

    # update experiment stats

    print "\n"


def reset_jobs(name, status):
    experiments = experiment_scheduler.load_experiments(
        cluster=None, filter_eq_dct=dict(name=name))

    if len(experiments) == 0:
        print "No experiments in database %s" % get_db_string("experiments")
        return

    experiment = experiments[0]

    if status == "broken":
        jobs = job_scheduler.load_broken_jobs(experiment["table"])
    elif status == "completed":
        jobs = job_scheduler.load_completed_jobs(experiment["table"])
    elif status == "running":
        jobs = job_scheduler.load_running_jobs(experiment["table"])

    print "Resetting %s jobs to START status..." % status
    job_scheduler.update_jobs(jobs, {sql.STATUS: sql.START, 'proc_status': 'pending'})


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


def main(argv):
    options = get_options(argv)

    if options.verbose:
        logging.basicConfig(level=logging.DEBUG, format=LOGGING_FORMAT)

    if options.command == LAUNCH:
        launch(options.cluster)
    if options.command == LAUNCH_LOCAL:
        launch_local(options.experiment_name, options.n, options.root)
    elif options.command == MONITOR:
        monitor(options.cluster)
    elif options.command == LIST:
        list_experiments(options.cluster)
    elif options.command == RESET:
        reset_jobs(options.experiment_name,
                   options.job_status)
    elif options.command == REMOVE:
        remove_experiment(options.name)


if __name__ == "__main__":
    main(sys.argv[1:])
