import enum

class Role(enum.Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2

class LeaseContext(enum.Enum):
    PRIMARY = 0
    SECONDARY = 1