import subprocess
import sys

from jobman import sql

from distributed_jobman import get_db_string
from distributed_jobman.database import database


JOBMAN_COMMAND_TEMPLATE = "jobman sql %(arguments)s %(db_string)s %(root)s"
JOBDISPATCH_COMMAND_TEMPLATE = "jobdispatch %(arguments)s %(jobman)s"


def _build_jobman_command_string(db_string, root, nb_of_jobs_to_launch=None):
    arguments_string = ""
    if nb_of_jobs_to_launch is not None:
        arguments_string += "-n %d" % nb_of_jobs_to_launch

    return (JOBMAN_COMMAND_TEMPLATE %
            dict(arguments=arguments_string, db_string=db_string, root=root))


def _build_jobdispatch_command_string(cluster, experiment, nb_of_jobs_to_launch):
    db_string = get_db_string(experiment["table"])

    jobman_command_string = _build_jobman_command_string(
        db_string, experiment["clusters"][cluster]["root"])

    arguments_string = ""
    if experiment.get("gpu", False):
        arguments_string += "--gpu"

    for option_name in ["duree", "mem", "env"]:
        arguments_string += (
            " --%s=%s" % (option_name, experiment["clusters"][cluster][option_name]))

    arguments_string += " --repeat_jobs=%d" % nb_of_jobs_to_launch

    command_string = (JOBDISPATCH_COMMAND_TEMPLATE %
                      dict(arguments=arguments_string,
                           jobman=jobman_command_string))

    return command_string


def _run_process(command_string):

    command_string
    process = subprocess.Popen([command_string],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True)

    if process.returncode is not None and process.returncode < 0:
        sys.stderr.write(process.stderr.read())
        return -1

    sys.stdout.write(process.stdout.read() + "\n")
    sys.stdout.write(process.stderr.read() + "\n")

    return 1


def is_pending(job):
    return job["jobman"]["status"] == sql.START


def is_running(job):
    return job["jobman"]["status"] == sql.RUNNING


def is_completed(job):
    return job["jobman"]["status"] == sql.DONE


def is_broken(job):
    return (job["jobman"]["status"] in [sql.ERR_RUN, sql.ERR_SYNC, sql.ERR_START])


def load_pending_jobs(table_name):

    jobs = load_jobs(table_name, {"jobman.status": sql.START})

    return jobs


def load_running_jobs(table_name):

    jobs = load_jobs(table_name, {"jobman.status": sql.RUNNING})

    return jobs


def load_completed_jobs(table_name):

    jobs = load_jobs(table_name, {"jobman.status": sql.DONE})

    return jobs


def load_broken_jobs(table_name):

    jobs = load_jobs(table_name)

    return [job for job in jobs if is_broken(job)]


def load_jobs(table_name, filter_eq_dct=None, job_id=None):

    jobs = database.load(table_name, filter_eq_dct, job_id)

    return jobs


def save_job(table_name, job_desc):

    job = database.save(table_name, job_desc)

    return job


def update_jobs(table_name, jobs, update_dict):
    return database.update(table_name, jobs, update_dict)


def delete_jobs(table_name, jobs):
    return database.delete(table_name, jobs)


def submit_job(cluster, experiment, nb_of_jobs_to_launch):

    return _run_process(_build_jobdispatch_command_string(
        cluster, experiment, nb_of_jobs_to_launch))


def submit_local_job(experiment, nb_of_jobs_to_launch, root):
    db_string = get_db_string(experiment["table"])

    return _run_process(_build_jobman_command_string(
        db_string, root, nb_of_jobs_to_launch))
