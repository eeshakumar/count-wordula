from map_reduce.task_type import TaskType
from uuid import uuid4
import grpc
import stub.dist_mr_pb2 as dist_mr_pb2
import stub.dist_mr_pb2_grpc as dist_mr_pb2_grpc

from map_reduce.task_status import TaskStatus


def do_map():
    print("Map Task completed...")
    return


def do_reduce():
    print("Reduce task completed...")
    return


def do_task(assigned_task):
    task_type = assigned_task.task_type
    if task_type == TaskType.MAP.value:
        do_map()
    elif task_type == TaskType.REDUCE.value:
        do_reduce()
    return


def main():
    worker_id = str(uuid4())
    print(worker_id)
    channel = grpc.insecure_channel("localhost" + ':50051')
    stub = dist_mr_pb2_grpc.MapReduceDriverStub(channel)
    i = 0
    task_status = TaskStatus.UNDEFINED.value
    # make some call
    while(i <= 2):
        assigned_task = stub.GetTask(
            dist_mr_pb2.WorkerStatus(
                worker_id=worker_id, worker_status=0, 
                task_status=task_status), wait_for_ready=True)
        print(assigned_task.task_id, assigned_task.input_filename)
        if not assigned_task.task_id:
            print("No More Tasks! Quitting....")
            exit(0)
        do_task(assigned_task)
        task_status = TaskStatus.COMPLETED.value
        i += 1


if __name__ == "__main__":
    main()