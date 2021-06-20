
import multiprocessing
from map_reduce.task_status import TaskStatus

from utils import collect_map_tasks, collect_reduce_tasks
from map_reduce.task_type import TaskType


class Driver(object):

    def __init__(self, N, M):
        self.N = N
        self.M = M
        self.map_task_files = []
        self.reduce_task_files = []
        self.task_status = multiprocessing.Manager().dict()

    def collect_tasks(self, task_type=TaskType.MAP):
        if task_type == TaskType.MAP:
            self.map_task_files = collect_map_tasks()
            self.build_task_status(task_type)
        elif task_type == TaskType.REDUCE:
            reduce_tasks = collect_reduce_tasks()
            self.build_task_status(reduce_tasks, task_type)
        else:
            raise AttributeError(f"Task Type {task_type} not found!")

    def build_task_status(self, task_type):
        # while building the task all are status 0, no_started
        self.task_status[TaskStatus.NOT_STARTED] = {}
        self.task_status[TaskStatus.NOT_STARTED][task_type] = []
        if task_type == TaskType.MAP:
            tasks = self.map_task_files
        elif task_type == TaskType.REDUCE:
            tasks = self.reduce_task_files
        else:
            raise AttributeError(f"Task Type {task_type} not found!")
        for task in tasks:
            self.task_status[TaskStatus.NOT_STARTED][task_type].append(
                {
                    "file_name": task,
                    "worked_id": None,
                }
            )

    def wait_for_map_tasks(self):
        # check if all tasks are in status 1, completed
        return sorted(self.map_task_files) == \
            sorted(self.task_status[TaskStatus.COMPLETED][TaskType.MAP])
 
    def wait_for_reduce_tasks(self):
        # check if all tasks are in status 1, completed
        return sorted(self.map_task_files) == \
            sorted(self.task_status[TaskStatus.COMPLETED][TaskType.REDUCE])