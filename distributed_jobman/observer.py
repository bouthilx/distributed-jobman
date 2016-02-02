import distributed_jobman.schedulers.jobs as job_scheduler
from distributed_jobman.parsers.qstat import qstat
# from distributed_jobman.parsers.showq import showq
from distributed_jobman import config

try:
    import nvidia
except ImportError:
    nvidia = None


def count_pending_jobs(table_name):
    return len(job_scheduler.load_pending_jobs(table_name))


def count_running_jobs(table_name):
    return len(job_scheduler.load_running_jobs(table_name))


def count_completed_jobs(table_name):
    return len(job_scheduler.load_completed_jobs(table_name))


def count_broken_jobs(table_name):
    return len(job_scheduler.load_broken_jobs(table_name))


def count_submitted_jobs(username=None):
    if config["scheduler"]["type"] == "multi-gpu":
        if nvidia is None:
            raise ImportError("pynvml is not installed")
        return sum(not free for free in nvidia.get_free_gpus().values())
    elif config["scheduler"]["type"] == "cluster":
        jobs = qstat(username)
        return sum(job['job_array_running'] for job in jobs)
        # jobs = showq(username)
        # return sum(len(job_array_ids) for job_array_ids in jobs.values())
