from enum import Enum


class ClusterMode(Enum):
    LOCAL = "local"
    REMOTE = "remote"
    NONE = None


def parse_cluster_mode(value):
    return ClusterMode(value)
