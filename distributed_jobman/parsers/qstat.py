from collections import defaultdict
import re
import subprocess
import sys

from distributed_jobman.utils import is_int


def qstat(username=None, job_id=None):
    if username is None:
        command = "qstat -f"
    else:
        command = "qstat -f -u %s" % username

    process = subprocess.Popen([command],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True)

    if process.returncode is not None and process.returncode < 0:
        sys.stderr.write(process.stderr.read())
        sys.exit(1)

    full_qstat = process.stdout.read()

    if username is None:
        command = "qstat -t"
    else:
        command = "qstat -t -u %s" % username

    process = subprocess.Popen([command],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True)

    if process.returncode is not None and process.returncode < 0:
        sys.stderr.write(process.stderr.read())
        sys.exit(1)

    row_qstat = process.stdout.read()

    jobs = parse_qstat(row_qstat, full_qstat)

    if job_id is not None:
        return filter(lambda job: job["id"] == job_id, jobs)
    else:
        return jobs


id_regex = re.compile("^[0-9]*")


def parse_qstat(row_qstat, full_qstat):

    row_counts = defaultdict(int)
    for row in filter(lambda a: a.strip(), row_qstat.split("\n")):
        if is_int(row[0]):
            row_counts[int(id_regex.search(row).group(0))] += 1

    jobs = []
    for job_desc in full_qstat.split("\n\n"):
        if job_desc.strip() == "":
            continue

        job = dict(id=job_desc.split("\n")[0].split(":")[-1].strip())

        last_key = None

        for line in job_desc.split("\n")[1:]:

            if line[0] == "\t":
                if last_key is None:
                    raise ValueError("qstat stdout is not formatted correctly")
                job[last_key] += line[1:]
            else:
                splits = line.split(" = ")
                key = splits[0].strip()
                value = " = ".join(splits[1:]).strip()
                job[key] = value

            last_key = key

        job['job_array_running'] = \
            row_counts[int(id_regex.search(job['id']).group(0))]

        jobs.append(job)

    return jobs
