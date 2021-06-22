from __future__ import print_function
import logging
import os
from utils import driver_arg_parse
import time
from map_reduce.driver import Driver
from map_reduce.driver_service import DriverService
import grpc
from concurrent import futures
import sys
import stub.dist_mr_pb2_grpc

def start_server(port, driver):
    # initialize service on host port
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    stub.dist_mr_pb2_grpc.add_MapReduceDriverServicer_to_server(
        DriverService(driver=driver), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print("-----------SERVER STARTED----------")
    return server


def stop_server(server, driver):
    print("-----------20s before shutdown----------")
    time.sleep(20)
    print("----------COMPLETED TASKS-----------")
    driver.print_completed_tasks_report()
    print("----------COMPLETED TASKS-----------")
    print("-----------SERVER STOPPED----------")
    server.stop(0)


def main(args):
    driver = Driver(args.n, args.m)
    server = start_server(50051, driver)
    while(True):
        if driver.all_tasks_complete():
            print("All tasks complete! Quitting...")
            stop_server(server, driver)
            exit(0)


if __name__ == "__main__":
    logging.basicConfig()
    parser = driver_arg_parse()
    args = parser.parse_args(sys.argv[1:])
    main(args)