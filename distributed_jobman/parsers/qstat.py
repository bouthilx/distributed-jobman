import subprocess
import sys


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

    jobs = parse_qstat(process.stdout.read())

    if job_id is not None:
        return filter(lambda job: job["id"] == job_id, jobs)
    else:
        return jobs


def parse_qstat(qstat_out_str):

    jobs = []
    for job_desc in qstat_out_str.split("\n\n"):
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

        jobs.append(job)

    return jobs
