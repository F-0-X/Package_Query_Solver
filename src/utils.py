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

    def __init__(self, df, representation_tuple):
        self.df = df
        self.representation_tuple = representation_tuple

    def get_df(self) -> pd.Dataframe:
        return self.df

    def get_representation_tuple(self) -> np.ndarray:
        return self.representation_tuple

    def get_titles(self):
        return self.df.columns
