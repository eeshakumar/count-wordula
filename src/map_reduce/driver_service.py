from map_reduce.task_type import TaskType
from map_reduce.task_status import TaskStatus

import stub.dist_mr_pb2
import stub.dist_mr_pb2_grpc


INPUT_DIR = "./input"
OUTPUT_DIR = "./out"
INTERMEDIATE_DIR = "./intermediate"


def build_task(task_type, next_task, N, M):
    # build task into Task obj
    return stub.dist_mr_pb2.Task(task_id=next_task.get("task_id", None),
            task_type=task_type, input_dir=INPUT_DIR,
            input_filename=next_task.get("file_name", ""),
            output_dir=OUTPUT_DIR, N=N, M=M)


class DriverService(stub.dist_mr_pb2_grpc.MapReduceDriverServicer):

    def __init__(self, driver, *args, **kwargs):
        self.driver = driver
        self.driver.collect_tasks()
        self.driver.collect_tasks(TaskType.REDUCE.value)
        # super().__init__(args, kwargs)

    def GetTask(self, request, context):
        print()
        print("Recieved REQ", request.worker_id, request.worker_status, request.task_status)
        task_type, prev_task = self.driver.get_prev_task_for_worker(request.worker_id, request.task_status)
        if prev_task is not None:
            print("PREV TASK", prev_task)
            self.driver.update_task_status(
                request.worker_id, prev_task["file_name"], TaskStatus.IN_PROGRESS.value, 
                task_type, prev_task["task_id"])
        task_type, next_task = self.driver.get_next_task()
        print("Sending next task", next_task)
        task = build_task(task_type, next_task, self.driver.N, self.driver.M)
        self.driver.update_task_status(request.worker_id, task.input_filename, 
                                       TaskStatus.NOT_STARTED.value, task.task_type,
                                       next_task.get("task_id", None))
        print(request.worker_id, next_task.get("file_name", None))
        return task
