import json
import os

import pandas as pd
import matplotlib.pyplot as plt

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

def getDataframe(query):
    data_dir = 'data/'
    table_name = query["table"]
    path_to_dataset = data_dir + table_name + '.csv'

    if not os.path.isfile(path_to_dataset):
        # TODO maybe we can choose to return empty query result
        raise Exception("can't find the table in the query")

    return pd.read_csv(path_to_dataset)

def setQueryTableName(query, table_name='tpch'):
    query['table'] = table_name
    return query

def splitDataset(size=[0.1, 0.4, 0.7], path_to_input_file='data/tpch.csv', randomSample=False):
    if not os.path.isfile(path_to_input_file):
        raise Exception("input file is not a file")

    # read dataset as dataframe
    dataset = pd.read_csv(path_to_input_file)
    # total number of rows in the dataset, or dataset.shape[0]
    row_cnt = len(dataset.index)  # 6001215 header not included

    for percent in size:
        file_path_name = 'data/tpch_' + str(percent * 100) + '%'
        if randomSample:
            file_path_name += '_rand.csv'
            dataset_part = dataset.sample(frac=percent, random_state=200)
        else:
            file_path_name += '.csv'
            dataset_part = dataset.iloc[:int(percent * row_cnt), :]  # percent% of the dataset
        # convert to csv
        dataset_part.to_csv(file_path_name, index=False)

def plotDirectVsSketchRefine(dataset_size, direct_time_taken, sketchRefine_time_taken, query_name, x_label, y_label):
    plt.plot(dataset_size, direct_time_taken)
    plt.plot(dataset_size, sketchRefine_time_taken)
    plt.title(query_name)
    plt.xlabel('Dataset size')
    plt.ylabel('Time')
    plt.legend(['Direct', 'SketchRefine'])
    plt.savefig('output/direct_vs_sketchRefine.png')
    # plt.show()

def plotDirect(dataset_size, direct_time_taken, query_name, x_label, y_label):
    dataset_size = [x / 10 for x in range(1, 11)]
    plt.plot(dataset_size, direct_time_taken)
    plt.title(query_name)
    plt.xlabel('Dataset size')
    plt.ylabel('Time')
    plt.legend(['Direct'])
    plt.savefig('output/direct_' + query_name + '.png')



class OptimizeObjective(Enum):
    MAXIMIZE = 1
    MINIMIZE = 2
    AVERAGE = 3


class LoadAndWrite:

    def __init__(self, args):
        self.partition_dir = args.temp_dir
        self.data_dir = args.data_dir
        self.output_dir = args.output_dir

    def load_csv(self, path):
        if not os.path.isfile(path):
            raise Exception("can't find the table in the query")
        return pd.read_csv(path, sep=',')

    # used by partition
    def load_initial_table(self, table_name):
        path_to_dataset = self.data_dir + table_name + '.csv'
        return self.load_csv(path_to_dataset)

    def store_partition(self, df, table_name, partition_option):
        file_path = self.partition_dir + table_name + "_" + partition_option + ".csv"
        df.to_csv(file_path, index=False)

    def already_partitioned(self, table_name, partition_option):
        file_path = self.partition_dir + table_name + "_" + partition_option + ".csv"
        return os.path.isfile(file_path)

    # This method is used by sketch to get the representation table
    def get_reprecentation(self, table_name, partition_core):
        # get path to file by table name and self.partition_dir
        file_path = self.partition_dir + table_name + "_representation_" + partition_core.core_name + ".csv"
        # read in corresponding csv
        return self.load_csv(file_path)

    def get_partition_group(self, table_name, partition_core, group_id):
        file_path = self.partition_dir + table_name + '_group' +\
            str(group_id) + "_" + partition_core.core_name + '.csv'
        return self.load_csv(file_path)

    def store_output(self, df, table_name, partition_core, is_direct_mode=False):
        if df is None:
            return
        mode = '_SR'
        if is_direct_mode:
            mode = '_D'
        file_path = self.output_dir + table_name + " " + partition_core.core_name + mode + '.csv'
        if os.path.isfile(file_path):
            os.remove(file_path)

        df.to_csv(file_path, index=False)

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

    def __init__(self, representation_tuple, group_df=None):
        self.representation_tuple = representation_tuple
        # self.representation_tuple.reset_index(drop=True)
        self.group_df = group_df
        self.group_id = int(representation_tuple['gid'])
        self.num_of_tuple = int(representation_tuple['num_of_tuple'])


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
        return self.group_df

    def get_representation_tuple(self):
        return self.representation_tuple

    def get_titles(self):
        return self.df.columns

    def get_group_id(self):
        return self.group_id

    def get_num_of_tuple(self):
        return self.num_of_tuple


