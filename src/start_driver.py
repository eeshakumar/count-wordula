from __future__ import print_function
import logging

from map_reduce.driver import Driver
from map_reduce.driver_service import DriverService
import grpc
from concurrent import futures

import stub.dist_mr_pb2_grpc

def start_server(port):
    # initialize service on host port
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    driver = Driver(4, 5)
    stub.dist_mr_pb2_grpc.add_MapReduceDriverServicer_to_server(
        DriverService(driver=driver), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print("server started")
    return server


def stop_server(self, server):
    # all tasks complete
    server.stop(0)    


def main():
    server = start_server(50051)
    server.wait_for_termination()
    return


if __name__ == "__main__":
    logging.basicConfig()
    main()