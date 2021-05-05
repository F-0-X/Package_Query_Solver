from sklearn.cluster import AgglomerativeClustering
import numpy as np
import pandas as pd
import os


def kd_tree(table_name, n_cluster, data_folder_path, temp_folder_path):
    path_to_dataset = data_folder_path + table_name + '.csv'

    if not os.path.isfile(path_to_dataset):
        # TODO maybe we can choose to return empty query result
        raise Exception("can't find the table in the query")

    # KMEANS cluster
    Table = pd.read_csv(path_to_dataset, sep=',')
    cluster_label = AgglomerativeClustering(n_clusters=n_cluster,linkage='complete').fit_predict(Table.to_numpy())
    dataframe_cluster = Table
    dataframe_cluster['gid'] = cluster_label
    file_path = temp_folder_path + table_name + "_clustered" + ".csv"
    dataframe_cluster.to_csv(file_path, index=False)

    unique, counts = np.unique(cluster_label, return_counts=True)
    print(dict(zip(unique, counts)))
    # represent = []
    # for i in range(n_cluster):
    #     df = dataframe_cluster.loc[dataframe_cluster['gid'] == i]
    #     file_path = temp_folder_path + table_name + "_cluster_" + str(i) + ".csv"
    #     df.to_csv(file_path, index=False)
    #     size = len(df)
    #     # calculate min, max, avg, and store each row of MIN, MAX in the representative csv file, the last row is MEAN
    #     df_np = df.to_numpy()
    #
    #     info = ["MIN", size]
    #     mini = np.min(df_np[:, :], axis=0)
    #     mini = np.append(mini, info)
    #
    #     info = ["MAX", size]
    #     maxi = np.max(df_np[:, :], axis=0)
    #     maxi = np.append(maxi, info)
    #
    #     info = ["MEAN", size]
    #     meanc = np.mean(df_np, axis=0)
    #     meanc = np.append(meanc, info)
    #
    #     represent.append(mini)
    #     represent.append(maxi)
    #     represent.append(meanc)
    # represent = np.array(represent)
    # represent[:, 0] = np.arange(1, len(represent)+1)
    # represent = np.array(represent)
    # re_col = list(dataframe_cluster.columns)
    # re_col.append("rpst")
    # re_col.append("g_size")
    # repre_df = pd.DataFrame(represent, columns=re_col)
    # rpst_file_path = temp_folder_path + table_name + "_representative" + ".csv"
    # repre_df.to_csv(rpst_file_path, index=False)


from timeit import default_timer as timer

start = timer()
a = kd_tree('tpch1000', 10, 'data/', 'temp/')
end = timer()
print("time is ", end - start)
