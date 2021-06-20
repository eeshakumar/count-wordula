import os


def collect_map_tasks():
    return os.listdir("/data/input")


def collect_reduce_tasks():
    return os.listdir("/data/intermediate")