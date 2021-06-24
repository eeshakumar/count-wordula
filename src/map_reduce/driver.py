import os
import uuid
import yaml
import multiprocessing
from itertools import product
from map_reduce.task_status import TaskStatus
from pathlib import Path

from utils import collect_map_tasks, collect_reduce_tasks
from utils import INPUT_DIR, OUTPUT_DIR, INTERMEDIATE_DIR
from map_reduce.task_type import TaskType
import stub.dist_mr_pb2 as dist_mr_pb2

class Driver(object):

    def __init__(self, N, M):
        self.N = N
        self.M = M
        self.map_task_files = []
        self.reduce_task_files = []
        self.task_status_manager = multiprocessing.Manager()
        self.task_status = self.task_status_manager.dict()

    def mr_file_storage(self):
        Path(INTERMEDIATE_DIR).mkdir(parents=True, exist_ok=True)
        for n, m in list(product(range(self.N), range(self.M))):
            path = f"mr-{n}-{m}"
            with open(os.path.join(INTERMEDIATE_DIR, path), 'w') as f:
                f.close()

        Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        for m in range(self.M):
            path = f"out-{m}"
            with open(os.path.join(OUTPUT_DIR, path), 'w') as f:
                f.close()

    def collect_tasks(self, task_type=TaskType.MAP.value):
        if task_type == TaskType.MAP.value:
            self.map_task_files = collect_map_tasks()
            self.build_task_status(task_type)
        elif task_type == TaskType.REDUCE.value:
            self.reduce_task_files = collect_reduce_tasks()
            self.build_task_status(task_type)
        else:
            raise AttributeError(f"Task Type {task_type} not found!")

    def build_task_status(self, task_type):
        # while building the task all are status 0, no_started
        new_tasks = self.task_status_manager.list()
        if task_type == TaskType.MAP.value:
            tasks = self.map_task_files
            for i, task in enumerate(tasks):
                new_task = {
                        "task_id": str(uuid.uuid4()),
                        "file_name": task,
                        "worked_id": None,
                        "map_id": i % self.N,
                    }
                new_tasks.append(new_task)
        elif task_type == TaskType.REDUCE.value:
            for m in range(self.M):
                new_task = {
                    "task_id": str(uuid.uuid4()),
                    "worked_id": None,
                    "reduce_id": m,
                }
                new_tasks.append(new_task)
        else:
            raise AttributeError(f"Task Type {task_type} not found!")
        
        if self.task_status.get(TaskStatus.NOT_STARTED.value, None) is None:
            self.task_status[TaskStatus.NOT_STARTED.value] = self.task_status_manager.dict()
        self.task_status[TaskStatus.NOT_STARTED.value][task_type] = new_tasks

        if self.task_status.get(TaskStatus.COMPLETED.value, None) is None:
            self.task_status[TaskStatus.COMPLETED.value] = self.task_status_manager.dict()
        self.task_status[TaskStatus.COMPLETED.value][task_type] = self.task_status_manager.list()

        if self.task_status.get(TaskStatus.IN_PROGRESS.value, None) is None:
            self.task_status[TaskStatus.IN_PROGRESS.value] = self.task_status_manager.dict()
        self.task_status[TaskStatus.IN_PROGRESS.value][task_type] = self.task_status_manager.list()

    def is_map_tasks_complete(self):
        # check if all tasks are in status 1, completed
        return len(self.map_task_files) == \
            len(self.task_status[TaskStatus.COMPLETED.value][TaskType.MAP.value])
 
    def is_reduce_tasks_complete(self):
        # check if all tasks are in status 1, completed
        return self.M == len(self.task_status[TaskStatus.COMPLETED.value][TaskType.REDUCE.value])

    def get_next_task(self):
        if not self.is_map_tasks_complete():
            task_type = TaskType.MAP.value
        else:
            task_type = TaskType.REDUCE.value

        try:
            next_task = self.task_status[TaskStatus.NOT_STARTED.value][task_type].pop(0)
        except IndexError:
            next_task = {}
        return task_type, next_task

    def update_task_status(self, worker_id, filename, old_status, task_type, task_id):
        if task_id is not None:
            if old_status == TaskStatus.NOT_STARTED.value:
                new_status = TaskStatus.IN_PROGRESS.value
            elif old_status == TaskStatus.IN_PROGRESS.value:
                new_status = TaskStatus.COMPLETED.value
        
            tasks = self.task_status[new_status][task_type]
            tasks.append({
                "task_id": task_id,
                "file_name": filename,
                "worker_id": worker_id
            })
            if self.task_status.get(new_status, None) is None:
                self.task_status[new_status]= {task_type: tasks}
            else:
                self.task_status[new_status][task_type] = tasks
        
    def get_prev_task_for_worker(self, worker_id, task_status):
        # if the status is undefined, the worker is just starting execution, 
        # and so attempts a map operation
        if task_status == TaskStatus.UNDEFINED.value:
            return TaskType.MAP.value, None
        prev_task = None
        for task_type in TaskType:
            in_progress_tasks = self.task_status[TaskStatus.IN_PROGRESS.value].get(task_type.value, 
                                                 self.task_status_manager.list())
            for i, in_progress_task in enumerate(in_progress_tasks):
                if worker_id == in_progress_task["worker_id"]:
                    try:
                        prev_task = self.task_status[TaskStatus.IN_PROGRESS.value][task_type.value].pop(i)
                        return task_type.value, prev_task
                    except IndexError:
                        break
        return task_type.value, prev_task
    
    def all_tasks_complete(self):
        return self.is_map_tasks_complete() & self.is_reduce_tasks_complete()

    def print_completed_tasks_report(self):
        print(self.task_status[TaskStatus.COMPLETED.value][0])
        print("----------------------------------------------")
        print(self.task_status[TaskStatus.COMPLETED.value][1])
