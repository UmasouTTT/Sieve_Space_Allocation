import copy
import sys
from bitarray.util import *
from index.util import *
import pyarrow.parquet as pp
from bitarray import bitarray
from index.learnedIndexDemo.segment import Segment
from param import args

class SpaceAllocator:
    def __init__(self, directory, columns):
        self.directory = directory
        self.columns = columns
        self.allocate_scores = {}

    def generate_per_file_allocator(self):
        files = os.listdir(self.directory)
        files = [self.directory + _ for _ in files]
        for file in files:
            self.allocate_scores[file] = {}
            for column in self.columns:
                self.allocate_scores[file][column] = self._file_score(file, column)

    def _file_score(self, file, column):
        data, num_of_row_groups = self.indexParquet(file, column)
        segments = self._generate_segments(data, num_of_row_groups)

        # desgin score from three aspects: gap, slope, change degree
        return 0

    def _generate_segments(self, data, num_of_row_groups):
        bak_segments = []

        segments = []
        rowgroups = []
        sl_high = sys.maxsize
        sl_low = 0
        segment = []
        rowgroup = bitarray(num_of_row_groups)
        rowgroup.setall(0)
        last_rgs = bitarray(num_of_row_groups)
        last_rgs.setall(0)
        y = 0
        prekey = None
        sort_data_keys = sorted(data.keys())
        total_gap = 0
        learned_gap = 0
        gap_list = []

        for key in sort_data_keys:
            belonged_rg = data[key]
            if 0 == len(segment):
                segment.append(key)
                rowgroup |= belonged_rg
                last_rgs = belonged_rg
                y = 0
            else:
                _y = y
                if prekey != None and key - prekey > 1:
                    total_gap += (key - prekey - 1)
                    gap_list.append(key - prekey - 1)
                    # _y += 2
                    if count_or(last_rgs, belonged_rg) > args.largegapth:
                        _y += 2
                    else:
                        _y += 1
                else:
                    if not subset(belonged_rg, last_rgs):
                        _y += 1
                _sl = _y / (key - segment[0])
                if _sl > sl_high or _sl < sl_low or (key - segment[-1]) > int(
                        args.sieve_gap_percent * (sort_data_keys[-1] - sort_data_keys[0])) or (
                        args.segment_error == 1 and _y != y):  # 最后一个条件新加的应对gap
                    # new segment
                    learned_gap += (key - segment[-1] - 1)
                    segments.append(segment)
                    rowgroups.append(rowgroup)
                    if len(segment) == 1:
                        tempsl = 1
                    else:
                        if y != 0:
                            tempsl = y / (segment[-1] - segment[0])
                        else:
                            tempsl = 1 / (segment[-1] - segment[0] + 1)
                    bak_segments.append(Segment(tempsl, (segment[0], segment[-1])))
                    sl_high = sys.maxsize
                    sl_low = 0
                    segment = [key]
                    rowgroup = copy.copy(belonged_rg)
                    last_rgs = belonged_rg
                    y = 0
                else:
                    # update
                    _sl_high = (_y + args.segment_error) / (key - segment[0])
                    _sl_low = (_y - args.segment_error) / (key - segment[0])
                    sl_high = min(sl_high, _sl_high)
                    sl_low = max(sl_low, _sl_low)
                    segment.append(key)
                    rowgroup |= belonged_rg
                    last_rgs = belonged_rg
                    y = _y
            prekey = key
        # not global gap
        if len(segment) == 1:
            tempsl = 1
        else:
            if y != 0:
                tempsl = y / (segment[-1] - segment[0])
            else:
                tempsl = 1 / (segment[-1] - segment[0] + 1)
        rowgroups.append(rowgroup)
        segments.append(segment)
        bak_segments.append(Segment(tempsl, (segment[0], segment[-1])))
        return bak_segments

    def indexParquet(self, file, column):
        """
        遍历self.file对应的parquet文件
        :return:
        datas：字典，key是self.column属性的值，value是值出现过的rowgroup集合
        _min：self.file中self.column属性的最小值
        _max：self.file中self.column属性的最大值
        num_of_row_groups：self.file中行组的个数
        """
        # get files
        # read data {column:{value:rowGroup}} todo: only numerical type
        datas = {}
        table = pp.ParquetFile(file)
        num_of_row_groups = table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = table.read_row_group(row_group_index, columns=[column])
            for record in row_group_contents.column(column):
                if str(record) == 'None':
                    continue
                record = getRecord(record)
                if record not in datas:
                    datas[record] = bitarray(num_of_row_groups)
                    datas[record].setall(0)
                datas[record][row_group_index] = 1
        return datas, num_of_row_groups



if __name__=="__main__":
    current_path = os.getcwd()
    directory = "../../dataset/Maps/"
    column = ["lon"]
    error = 70000

    print("test on column {}".format(column))
    args.segment_error = error

    allocator = SpaceAllocator(directory, column)

    allocator.generate_per_file_allocator()




