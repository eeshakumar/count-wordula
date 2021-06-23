# count-wordula

## Distributed map reduce program to do word count with gRPC

A distributed map reduce implementation for word counting using gRPC for communication.

### Assumptions

- Task is allocated to a worker upon request. Hence Worker is client and DriverService is the Service.
- Given a file, worker knows where to look to obtain the input file.
- Once a task is alloted to a worker, the worker must finish the existing task before requesting for the next.
- Since worker doesnt respond to the task processed, driver treats assigned task as complete. However, worker can pass the previous task status to the worker.
- Workers are configured to a default driver port.
- Since only std libs usage is allowed, driver task allocation multiprocessing is handled by python's multiprocessing dict and list structures.

### Components

- DriverService
  
  A gRPC service that manages communication with worker for assigning tasks. It contains one API GetTask which uses the driver to obtain a new task for a worker.

- Driver
  
  Driver is part of the DriverService and manages the allocation of tasks to worker. The driver decides which worker gets which task. The driver also manages the task status and determines when a task is in progress and complete. The driver does not assign more than one task to one worker at any point in time. Driver also manages the file storage system.

- Worker
  
  Worker is the program that contains the client stub for communication with DriverService. The worker is only responsible for either map or reduce operation. Depending on the task type passed by driver, the worker perform only one task at a time.

- Task

  An object containing task information. These include unique id for the task, the requesting worker id, type of task, task specific id (map or reduce id) and input filename(if necessary).

### Setup

Once the repo is cloned, please create the virtual env, install pkgs and activate the venv. Setup might be necessary to register the package.

### Driver Worker Execution

- Open multiple terminal windows.
- The driver script can be started with start_driver.py
- The worker scripts can be started with start_worker.py
- The worker should print task request id and operation as it processes the tasks.
- Once complete, the driver will wait 20s and shutdown.
- The workers will quit when they do not recieve new tasks from the driver (after retries).

### Tests

Tests are present in test to verify the correctness of the map and reduce operations.
