
from collections import defaultdict
import re
import subprocess
import sys

from distributed_jobman.utils import is_int


def showq(username=None):
    if username is None:
        command = "showq"
    else:
        command = "showq -u %s" % username

    process = subprocess.Popen([command],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True)

    if process.returncode is not None and process.returncode < 0:
        sys.stderr.write(process.stderr.read())
        sys.exit(1)

    showq = process.stdout.read()

    jobs = parse_showq(showq)

    return jobs


job_id_regex = re.compile("^[0-9]*")
job_array_id_regex = re.compile("\[[0-9]*\]")


def _fetch_ids(rows, row_ids):
    for row in rows:
        job_id = int(job_id_regex.search(row).group(0))
        job_array_id_found = job_array_id_regex.search(row)
        if job_array_id_found:
            job_array_id = job_array_id_found.group(0).strip("[]")
        else:
            job_array_id = 0

        row_ids[job_id].add(job_array_id)


_showq_categories = ["ACTIVE JOBS", "IDLE JOBS", "BLOCKED JOBS"]


def parse_showq(showq_out_str):

    row_ids = defaultdict(set)
    rows = showq_out_str.split("\n")

    row_i = 0
    row_j = 0
    while row_i < len(rows):
        if rows[row_i].strip("-") in _showq_categories:
            row_i += 3
            row_j = row_i
            while rows[row_j].strip() != "":
                row_j += 1

            _fetch_ids(rows[row_i:row_j], row_ids)

            row_i = row_j
        else:
            row_i += 1

    return dict(row_ids)

if __name__ == "__main__":
    out = parse_showq(open("/home/bouthilx/showq.txt", 'r').read())

    print "\n".join(sorted([str((i, list(s))) for i, s in out.iteritems()]))
