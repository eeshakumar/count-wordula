from enum import Enum

class TaskStatus(Enum):

    NOT_STARTED = 0
    IN_PROGRESS = 1
    COMPLETED = 2
    UNDEFINED = -1