import json
import os
from enum import Enum
from heapq import heappush, heappop
from queue import Queue
import numpy as np
import pandas as pd


def processInputFile(path_to_input_file):
    if not os.path.isfile(path_to_input_file):
        raise Exception("input file is not a file")

    data = None
    with open(path_to_input_file) as json_file:
        data = json.load(json_file)
    if len(data['A']) != len(data['L']) or len(data['A']) != len(data['U']):
        raise Exception('invalid input file')
    return data


class OptimizeObjective(Enum):
    MAXIMIZE = 1
    MINIMIZE = 2
    AVERAGE = 3


class LoadAndWrite:

    def __init__(self, args):
        self.partition_dir = args.temp_dir

    # This method is used by sketch to get the representation table
    def getReprecentation(table_name, objective=OptimizeObjective.MAXIMIZE):
        # TODO get path to file by table name and self.partition_dir

        # TODO read in corresponding csv

        # TODO filter the df to get the representation table and return it
        a = 1

    # TODO a helper function which can help us generate the name of partition file


class SimplePQ:

    # failed_groups is a set
    # high_pq and low_pq are queue.Queue
    def __init__(self, failed_groups, high_pq, low_pq):
        self.failed_groups = failed_groups
        self.high_pq = high_pq
        self.low_pq = low_pq

    def prioritize(self, new_failed_groups):
        for group in new_failed_groups:
            if group not in self.failed_groups:
                self.failed_groups.add(group)
                self.high_pq.put(group)

    def pop(self):

        if not self.high_pq.empty():
            return self.high_pq.get()
        else:
            while not self.low_pq.empty():
                candidate = self.low_pq.get()
                if candidate not in self.failed_groups:
                    return candidate
        return None


class GroupAndRepresentationTuple:

    def __init__(self, group_id, group_df=None, representation_tuple=None, num_of_tuple=1):
        self.group_df = group_df
        self.group_id = group_id
        self.representation_tuple = representation_tuple
        self.num_of_tuple = num_of_tuple
        self.representation_tuple.reset_index(drop=True)

    def __eq__(self, other):
        return self.group_id == other.group_id

    def __ne__(self, other):
        return self.group_id != other.group_id

    def __lt__(self, other):
        return self.group_id < other.group_id

    def __le__(self, other):
        return self.group_id <= other.group_id

    def __gt__(self, other):
        return self.group_id > other.group_id

    def __ge__(self, other):
        return self.group_id >= other.group_id

    def __str__(self):
        return str(self.group_id)

    def __hash__(self):
        return hash(self.group_id)

    def __repr__(self):
        return str(self.group_id)

    def get_group_df(self):
        return self.df

    def get_representation_tuple(self):
        return self.representation_tuple

    def get_titles(self):
        return self.df.columns

    def get_group_id(self):
        return self.group_id

    def get_num_of_tuple(self):
        return self.num_of_tuple


# class RefiningPackage:
#
#     def __init__(self, unrefined_representation_df):
#         self.df1 = unrefined_representation_df
#         df1_columns = list(unrefined_representation_df.columns)
#         df2_columns = df1_columns[0:-2]
#         self.df2 = pd.DataFrame(columns=df2_columns)
#
#     def df1_refine_switch(self, group_id):
#         target_row = self.df1.loc[group_id-1, :]
#         if int(target_row['group_id']) != group_id:
#             target_row = self.df1[self.df1['group_id'] == group_id]