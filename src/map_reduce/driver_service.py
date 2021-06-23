from map_reduce.task_type import TaskType
from map_reduce.task_status import TaskStatus

import stub.dist_mr_pb2
import stub.dist_mr_pb2_grpc

from utils import INPUT_DIR, OUTPUT_DIR, INTERMEDIATE_DIR


def build_task(task_type, next_task, N, M):
    # build task into Task request
    return stub.dist_mr_pb2.Task(task_id=next_task.get("task_id", None),
            task_type=task_type, input_dir=INPUT_DIR,
            input_filename=next_task.get("file_name", ""),
            output_dir=OUTPUT_DIR, N=N, M=M, 
            map_id=next_task.get("map_id", -1),
            reduce_id=next_task.get("reduce_id", -1))


class DriverService(stub.dist_mr_pb2_grpc.MapReduceDriverServicer):

    def __init__(self, driver, *args, **kwargs):
        self.driver = driver
        self.driver.mr_file_storage()
        self.driver.collect_tasks()
        self.driver.collect_tasks(TaskType.REDUCE.value)

    def GetTask(self, request, context):
        task_type, prev_task = self.driver.get_prev_task_for_worker(
            request.worker_id, request.task_status)
        if prev_task is not None:
            self.driver.update_task_status(
                request.worker_id, prev_task["file_name"], TaskStatus.IN_PROGRESS.value, 
                task_type, prev_task["task_id"])
        task_type, next_task = self.driver.get_next_task()
        task = build_task(task_type, next_task, self.driver.N, self.driver.M)
        self.driver.update_task_status(request.worker_id, task.input_filename, 
                                       TaskStatus.NOT_STARTED.value, task.task_type,
                                       next_task.get("task_id", None))
        return task
