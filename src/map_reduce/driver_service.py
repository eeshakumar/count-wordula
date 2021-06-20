
import stub.dist_mr_pb2
import stub.dist_mr_pb2_grpc


class DriverService(stub.dist_mr_pb2_grpc.MapReduceDriverServicer):

    def __init__(self, driver, *args, **kwargs):
        self.driver = driver
        # super().__init__(args, kwargs)

    def GetTask(self, request, context):
        print("Recieved REQ", request.worker_id, request.worker_status)
        return stub.dist_mr_pb2.Task(task_id=1,
            task_type=0, input_dir='./input',
            input_filename='test_filename',
            output_dir='', N=3, M=4)

