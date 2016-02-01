import cPickle
import matplotlib.pyplot as plt
import numpy


def plot(jobs):
    x_key = "params.learning_rate.values"
    y_key = "valid_deterministic_misclassification_rate_mean_min"
    jobs.sort(key=lambda a: a[x_key])
    xs = []
    ys = []
    for job in jobs:
        y_value = job.get(y_key, None)
        if y_value:
            xs.append(job[x_key])
            ys.append(y_value)

    x = []
    y = []
    i = 0
    width = int(len(jobs) * 0.1)
    print width
    while i + width < len(jobs):
        argmin = numpy.array(xs[i:i + width]).argmin()
        x.append(xs[i:i + width][argmin])
        y.append(ys[i:i + width][argmin])
        i += 1

    return _plot(x, y)


def _plot(x, y):
    plt.plot(x, y)
    plt.show()
    return

if __name__ == "__main__":

    jobs = cPickle.load(open("test_jobs.pkl", 'rb'))
    plot(jobs)
