import os.path
from typing import List, Set
import pickle

from segment import Segment

class SieveIndex:
    def __init__(self, segments: List[Segment]):
        self.segments = segments

    def point_search(self, key: int) -> Set[str]:
        l, r = 0, len(self.segments) - 1

        segment = None
        while l < r:
            m = (l + r) // 2
            s = self.segments[m]
            if key < s.min_key:
                r = m - 1
            elif key > s.max_key:
                l = m + 1
            else:
                segment = s
                break

        s = self.segments[l]
        if segment is None and 0 <= l < len(self.segments) and s.min_key <= key <= s.max_key:
            segment = s
        if segment is None:
            return set()

        partition = segment.get_partition(key)
        if partition is not None:
            return partition.get_values()
        else:
            return set()

    def save(self, directory: str):
        for segment in self.segments:
            segment.keys = []
            filename = f'{segment.min_key}_{segment.max_key}.segment'
            with open(os.path.join(directory, filename), 'wb') as file:
                pickle.dump(segment, file)
