from sklearn.cluster import AgglomerativeClustering
from sklearn.cluster import DBSCAN
import numpy as np
import pandas as pd
import os
from sklearn.datasets import make_blobs
from sklearn.utils import check_array
from sklearn.utils.extmath import safe_sparse_dot
from sklearn.metrics.pairwise import pairwise_distances_argmin
# from memory_profiler import profile

from sklearn.mixture import GaussianMixture

def gaussian_cluster(table_name, n_cluster, data_folder_path, temp_folder_path):
    path_to_dataset = data_folder_path + table_name + '.csv'

    if not os.path.isfile(path_to_dataset):
        # TODO maybe we can choose to return empty query result
        raise Exception("can't find the table in the query")


    Table = pd.read_csv(path_to_dataset, sep=',')
    cluster_label = GaussianMixture(n_components=n_cluster).fit_predict(Table.to_numpy())

    dataframe_cluster = Table
    dataframe_cluster['gid'] = cluster_label
    file_path = temp_folder_path + table_name + "_clustered" + ".csv"
    # dataframe_cluster.to_csv(file_path, index=False)

    # See the frequency for each label
    unique, counts = np.unique(cluster_label, return_counts=True)
    print(dict(zip(unique, counts)))


from timeit import default_timer as timer

start = timer()
a = gaussian_cluster('tpch_1.0%_rand', 20, 'data/', 'temp/')
end = timer()
print("time is ", end - start)
