from enum import Enum

class TaskType(Enum):

    MAP = 0
    REDUCE = 1
    UNDEFINED = -1