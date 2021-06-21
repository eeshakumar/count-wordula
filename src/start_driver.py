from __future__ import print_function
import logging
import os
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
    return driver, server


def stop_server(server):
    server.stop(0)    


def main():
    driver, server = start_server(50051)
    while(True):
        if driver.all_tasks_complete():
            print("All tasks complete! Quitting....")
            stop_server(server)
            exit(0)


if __name__ == "__main__":
    print("Current DIR", os.getcwd())
    logging.basicConfig()
    main()