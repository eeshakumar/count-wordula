import grpc
import stub.dist_mr_pb2 as dist_mr_pb2
import stub.dist_mr_pb2_grpc as dist_mr_pb2_grpc


def do_task():
    return

def main():
    channel = grpc.insecure_channel("localhost" + ':50051')
    stub = dist_mr_pb2_grpc.MapReduceDriverStub(channel)
    # make some call
    while(True):
        assigned_task = stub.GetTask(
            dist_mr_pb2.WorkerStatus(worker_id="w_id_1", worker_status=0))
        print(assigned_task.task_id)
        if assigned_task.task_id == None:
            exit(0)
        do_task()
    return


if __name__ == "__main__":
    main()