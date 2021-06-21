import os


def collect_map_tasks():
    return os.listdir(os.path.join(os.getcwd(), "data/inputs"))


def collect_reduce_tasks():
    return os.listdir(os.path.join(os.getcwd(), "data/intermediate"))