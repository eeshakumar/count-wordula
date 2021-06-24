from utils import INTERMEDIATE_DIR, OUTPUT_DIR
from map_reduce.task_type import TaskType
from uuid import uuid4
import grpc
import stub.dist_mr_pb2 as dist_mr_pb2
import stub.dist_mr_pb2_grpc as dist_mr_pb2_grpc

from map_reduce.task_status import TaskStatus
from utils import do_map, do_reduce


def do_task(assigned_task):
    task_type = assigned_task.task_type
    if task_type == TaskType.MAP.value:
        print(f"Recieved map task: {assigned_task.task_id} "
              f"with ID: {assigned_task.map_id} "
              f"for: {assigned_task.input_filename}")
        do_map(assigned_task.input_filename,
               assigned_task.M, assigned_task.map_id)
    elif task_type == TaskType.REDUCE.value:
        print(f"Recieved reduce task: {assigned_task.task_id} "
              f"with ID: {assigned_task.reduce_id}")
        do_reduce(assigned_task.reduce_id, assigned_task.N)
    return


def main():
    worker_id = str(uuid4())
    print("WORKER ID:", worker_id)
    channel = grpc.insecure_channel("localhost" + ':50051')
    stub = dist_mr_pb2_grpc.MapReduceDriverStub(channel)
    retry = 3
    task_status = TaskStatus.UNDEFINED.value
    # make some call
    while(True):
        assigned_task = stub.GetTask(
            dist_mr_pb2.WorkerStatus(
                worker_id=worker_id, worker_status=0, 
                task_status=task_status), wait_for_ready=True)
        if not assigned_task.task_id:
            print("No tasks recieved. Retrying....")
            retry -= 1
            task_status = TaskStatus.UNDEFINED
            if retry == 0:
                print("No More Tasks, retries exhausted. Quitting...")
                exit(0)
        do_task(assigned_task)
        # task is assumed complete
        task_status = TaskStatus.COMPLETED.value


if __name__ == "__main__":
    main()