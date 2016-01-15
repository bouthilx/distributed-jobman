import distributed_jobman.schedulers.jobs as job_scheduler
from distributed_jobman.parsers.qstat import qstat


def count_pending_jobs(table_name):
    return len(job_scheduler._load_pending_jobs(table_name))


def count_running_jobs(table_name):
    return len(job_scheduler._load_running_jobs(table_name))


def count_completed_jobs(table_name):
    return len(job_scheduler._load_completed_jobs(table_name))


def count_broken_jobs(table_name):
    return len(job_scheduler._load_broken_jobs(table_name))


def _count_array_request(job):
    if "array_request" not in job:
        return 1

    array_request = job["job_array_request"]
    ids = array_request.split("-")
    if len(ids) != 2:
        raise ValueError("Invalid array request: %s" % array_request)

    return int(ids[1]) - int(ids[0]) + 1


def count_submitted_jobs(username=None):
    jobs = qstat(username)
    return sum(_count_array_request(job) for job in jobs)
