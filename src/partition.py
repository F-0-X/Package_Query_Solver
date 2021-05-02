from enum import Enum


# The partition_core is a object with partition rules
# we call partition_core.parts() with the initial dataframe as the parameter to get the result
# Then we call the store the groups in the temp_folder_path
def partition(partition_core, table_name, data_folder_path, temp_folder_path):
    # TODO based on the table name and data_folder_path to load the whole dataframe

    # TODO use the call the partition_core.partition to get the partitioned dataframe

    # TODO store the result to the temp folder, we not only store the partitions
    #  but also store the minimum, maximum and average value of all the partitioning attributes

    a = 1


class PartitionMode(Enum):
    SIZE_THRESHOLD = 1
    DIAMETER_BOUND = 2
    # HYBRID MODE means use the both size_threshold and diameter_bound as the standard for partitioning
    HYBRID = 3


class QuadTreePartitionCore:

    def __init__(self, size_threshold=None, diameter_bound=None, partition_mode=PartitionMode.SIZE_THRESHOLD):
        self.size_threshold = size_threshold
        self.diameter_bound = diameter_bound
        self.partition_mode = partition_mode

    # This method is a recursive method
    def partition(self, df):
        # list of partitioned dataframe
        result = []
        need_partition = self.need_partition(df)
        if need_partition:
            # TODO calculate the central point
            # TODO partition the df into 2^k small dataframe
            # TODO recursively call itself and add the dataframes in the results to result list
            a = 1
        else:
            result.append(df)
        return result

    def need_partition(self, df):
        result = False
        if self.partition_mode == PartitionMode.SIZE_THRESHOLD or \
                self.partition_mode == PartitionMode.HYBRID:
            # TODO raise an Execption if self.size_threshold is None
            # TODO compare the size of df with self.size_threshold and modify the result
            result = True
        if self.partition_mode == PartitionMode.DIAMETER_BOUND or \
                self.partition_mode == PartitionMode.HYBRID:
            # TODO raise an Execption if self.diameter_bound is None
            # TODO compare the diameter of each column of df with self.diameter_bound and modify the result
            result = True

        return result
