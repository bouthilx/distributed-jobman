from collections import defaultdict
import time

from pynvml import (
    nvmlInit,
    nvmlDeviceGetCount,
    nvmlDeviceGetHandleByIndex,
    nvmlDeviceGetName,
    nvmlDeviceGetTemperature,
    nvmlShutdown)


def count_gpus():
    nvmlInit()
    count = nvmlDeviceGetCount()
    nvmlShutdown()
    return count


def get_gpu_temperatures():
    nvmlInit()
    gpus = dict()
    for i in range(nvmlDeviceGetCount()):
        handle = nvmlDeviceGetHandleByIndex(i)
        gpus[i] = int(nvmlDeviceGetTemperature(handle, 0))

    nvmlShutdown()
    return gpus


def get_free_gpus():
    free_gpus = defaultdict(int)

    for i in range(10):
        for gpu_id, temperature in get_gpu_temperatures().iteritems():
            free_gpus[gpu_id] = max(free_gpus[gpu_id], temperature)

        time.sleep(1)

    for gpu_id, temperature in free_gpus.iteritems():
        free_gpus[gpu_id] = temperature < 70

    return dict(free_gpus)


if __name__ == "__main__":
    print get_gpu_temperatures()
    print get_free_gpus()
