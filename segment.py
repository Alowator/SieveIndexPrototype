import math
from typing import List, Set

class Partition:
    def __init__(self):
        self.values = set()

    def add_values(self, values: Set[str]):
        self.values = self.values.union(values)

    def get_values(self) -> Set[str]:
        return self.values

class Segment:
    def __init__(self, true_tbc: int, min_key: int, max_key: int, keys: List[int]):
        self.true_tbc = true_tbc
        self.min_key = min_key
        self.max_key = max_key
        self.keys = keys

        self.partitions: List[Partition] = []
        self.partition_size = 0

    def get_slope(self) -> float:
        if self.true_tbc != 0:
            return self.true_tbc / (self.max_key - self.min_key)
        else:
            return 1 / (self.max_key - self.min_key + 1)

    def get_width(self) -> int:
        return self.max_key - self.min_key

    def get_keys(self) -> List[int]:
        return self.keys

    def get_true_tbc(self) -> int:
        return self.true_tbc

    def build_middle_layer(self, partition_size: int):
        self.partition_size = max(partition_size, 1)
        partitions_num = math.ceil((self.get_width() + 1) / self.partition_size)
        self.partitions = [Partition() for _ in range(partitions_num)]

    def get_partition(self, key) -> Partition | None:
        if self.min_key <= key <= self.max_key:
            return self.partitions[(key - self.min_key) // self.partition_size]
        else:
            return None
