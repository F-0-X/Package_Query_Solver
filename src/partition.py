from enum import Enum
from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import os


# The partition_core is a object with partition rules
# we call partition_core.parts() with the initial dataframe as the parameter to get the result
# Then we call the store the groups in the temp_folder_path
def partition(partition_core, table_name, load_write_helper):
    # skip the partition process if we already do the partition
    if load_write_helper.already_partitioned(table_name, partition_core.core_name):
        print("Skip Partition(we have already partition this table with this partition core)")
        return

    # based on the table name and data_folder_path to load the whole dataframe
    Table = load_write_helper.load_initial_table(table_name)

    # call the partition_core.partition to get the partitioned dataframe
    dataframe_cluster, groups, repre_df = partition_core.partition(Table)

    #  store the result to the temp folder, we not only store the partitions
    #  but also store the minimum, maximum and average value of all the partitioning attributes
    load_write_helper.store_partition(dataframe_cluster, table_name, partition_core.core_name, "clustered")

    for i, dfi in enumerate(groups):
        load_write_helper.store_partition(dfi, table_name, partition_core.core_name, "group" + str(i))

    load_write_helper.store_partition(repre_df, table_name, partition_core.core_name, "representation")


class KmeansPartitionCore:

    def __init__(self, n_cluster):
        self.n_cluster = n_cluster
        self.core_name = "Kmeans" + str(n_cluster)

    def partition(self, Table):
        # KMEANS cluster
        cluster_label = KMeans(n_clusters=self.n_cluster).fit_predict(Table.to_numpy())
        dataframe_cluster = Table
        dataframe_cluster['gid'] = cluster_label
        # file_path = temp_folder_path + table_name + "_clustered" + ".csv"
        # dataframe_cluster.to_csv(file_path, index=False)

        represent = []
        groups = []
        for i in range(self.n_cluster):
            df = dataframe_cluster.loc[dataframe_cluster['gid'] == i]
            groups.append(df)
            # file_path = temp_folder_path + table_name + "_cluster_" + str(i) + ".csv"
            # df.to_csv(file_path, index=False)
            size = len(df)
            # calculate min, max, avg, and store each row of MIN, MAX in the representative csv file, the last row is MEAN
            df_np = df.to_numpy()

            info = ["MIN", size]
            mini = np.min(df_np[:, :], axis=0)
            mini = np.append(mini, info)

            info = ["MAX", size]
            maxi = np.max(df_np[:, :], axis=0)
            maxi = np.append(maxi, info)

            info = ["MEAN", size]
            meanc = np.mean(df_np, axis=0)
            meanc = np.append(meanc, info)

            represent.append(mini)
            represent.append(maxi)
            represent.append(meanc)

        represent = np.array(represent)
        # represent[:, 0] = np.arange(1, len(represent) + 1)
        # represent = np.array(represent)
        re_col = list(dataframe_cluster.columns)
        re_col.append("rpst")
        re_col.append("g_size")
        repre_df = pd.DataFrame(represent, columns=re_col)
        # rpst_file_path = temp_folder_path + table_name + "_representative" + ".csv"
        # repre_df.to_csv(rpst_file_path, index=False)

        return dataframe_cluster, groups, repre_df


class GaussianParitionCore:
    def __init__(self, n_cluster):
        self.n_cluster = n_cluster
        self.core_name = "Gaussian" + str(n_cluster)

    def partition(self, Table):
        # Gaussian cluster
        cluster_label = KMeans(n_clusters=self.n_cluster, random_state=0).fit_predict(Table.to_numpy())
        dataframe_cluster = Table
        dataframe_cluster['gid'] = cluster_label

        represent = []
        groups = []
        for i in range(self.n_cluster):
            df = dataframe_cluster.loc[dataframe_cluster['gid'] == i]
            groups.append(df)
            # file_path = temp_folder_path + table_name + "_cluster_" + str(i) + ".csv"
            # df.to_csv(file_path, index=False)
            size = len(df)
            # calculate min, max, avg, and store each row of MIN, MAX in the representative csv file, the last row is MEAN
            df_np = df.to_numpy()

            info = ["MIN", size]
            mini = np.min(df_np[:, :], axis=0)
            mini = np.append(mini, info)

            info = ["MAX", size]
            maxi = np.max(df_np[:, :], axis=0)
            maxi = np.append(maxi, info)

            info = ["MEAN", size]
            meanc = np.mean(df_np, axis=0)
            meanc = np.append(meanc, info)

            represent.append(mini)
            represent.append(maxi)
            represent.append(meanc)

        represent = np.array(represent)
        # represent[:, 0] = np.arange(1, len(represent) + 1)
        # represent = np.array(represent)
        re_col = list(dataframe_cluster.columns)
        re_col.append("rpst")
        re_col.append("g_size")
        repre_df = pd.DataFrame(represent, columns=re_col)
        return dataframe_cluster, groups, repre_df

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
