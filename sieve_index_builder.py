import os
import pickle
import sys
from typing import Dict, Any, Set, List

import pyarrow.parquet as pp

from segment import Segment
from sieve_index import SieveIndex

class SieveIndexBuilder:
    def __init__(self, table_path: str):
        self.table_path = table_path
        self.data_files = []
        for f in os.listdir(table_path):
            if os.path.isfile(os.path.join(table_path, f)):
                self.data_files.append(f)
        self.data_files = self.data_files[:10]

    def build_index(self, column_name: str) -> SieveIndex:
        data = self._get_data(column_name)
        segments = self._segment_data(data)

        for segment in segments:
            partition_size = segment.get_width() // (segment.get_true_tbc() + 1)
            segment.build_middle_layer(partition_size)
            for key in segment.get_keys():
                partition = segment.get_partition(key)
                partition.add_values(data[key])

        return SieveIndex(segments)

    @classmethod
    def load_index(cls, directory: str) -> SieveIndex:
        files = sorted(os.listdir(directory))
        segments = []
        for filename in files:
            with open(os.path.join(directory, filename), 'rb') as file:
                segments.append(pickle.load(file))
        return SieveIndex(segments)


    def _segment_data(self, data) -> List[Segment]:
        sieve_gap_percent = 0.01
        segment_error = 100

        segments = []
        segmentsC = []
        sort_data_keys = sorted(data.keys())

        segment = []
        belongs_range = set()
        last_belongs_range = set()
        y = 0
        prekey = None
        total_gap = 0
        learned_gap = 0
        gap_list = []
        sl_high = sys.maxsize
        sl_low = 0
        for key in sort_data_keys:
            key_belongs_range = data[key]
            if len(segment) == 0:
                segment = [key]
                [belongs_range.add(x) for x in key_belongs_range]
                last_belongs_range = belongs_range
                y = 0
            else:
                _y = y
                if prekey != None and key - prekey > 1:
                    total_gap += (key - prekey - 1)
                    gap_list.append(key - prekey - 1)
                    _y += 1
                    # if count_or(last_rgs, belonged_rg) > args.largegapth:
                    #     _y += 2
                    # else:
                    #     _y += 1
                else:
                    if not belongs_range.issubset(last_belongs_range):
                        _y += 1
                _sl = _y / (key - segment[0])
                if _sl > sl_high \
                        or _sl < sl_low \
                        or (key - segment[-1]) > int(sieve_gap_percent * (sort_data_keys[-1] - sort_data_keys[0])) \
                        or (segment_error == 1 and _y != y):
                    learned_gap += (key - segment[-1] - 1)
                    segments.append(segment)
                    if len(segment) == 1:
                        tempsl = 1
                    else:
                        if y != 0:
                            tempsl = y / (segment[-1] - segment[0])
                        else:
                            tempsl = 1 / (segment[-1] - segment[0] + 1)
                    segmentsC.append(Segment(y, segment[0], segment[-1], segment))
                    sl_high = sys.maxsize
                    sl_low = 0
                    segment = [key]
                    last_rgs = belongs_range
                    y = 0
                else:
                    # update
                    _sl_high = (_y + segment_error) / (key - segment[0])
                    _sl_low = (_y - segment_error) / (key - segment[0])
                    sl_high = min(sl_high, _sl_high)
                    sl_low = max(sl_low, _sl_low)
                    segment.append(key)
                    last_rgs = belongs_range
                    y = _y
            prekey = key
        segments.append(segment)
        segmentsC.append(Segment(y, segment[0], segment[-1], segment))
        return segmentsC

    def _get_data_file_path(self, data_file: str) -> str:
        return os.path.join(self.table_path, data_file)

    def _get_data(self, column_name: str) -> Dict[Any, Set[str]]:
        data = {}
        for data_file in self.data_files:
            parquet_file = pp.ParquetFile(self._get_data_file_path(data_file))
            num_row_groups = parquet_file.num_row_groups
            for row_group_index in range(num_row_groups):
                row_group = parquet_file.read_row_group(row_group_index, columns=[column_name])
                for record in row_group.column(column_name):
                    value = record.as_py()
                    if data.get(value) is None:
                        data[value] = {data_file}
                    else:
                        data[value].add(data_file)

        return data
