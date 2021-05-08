import numpy as np
import pandas as pd

from src.Direct import *
from src.utils import *
from src.SketchRefine import *
import timeit
import re

def computeTimeCost(queries=[2], size=[0.1, 0.4, 0.7], randomSample=True, directMode=True):
    if directMode:
        mode = 'Direct'
    else:
        mode = 'Sketch_Refine'

    path_to_dataset = "data/"

    path_to_input_file = "input/Q2.json"
    query = processInputFile(path_to_input_file)

    dataset_size_list = [str(int(s * 100)) + '.0%' for s in size]
    # dataset_size_list looks like:
    # ['10.0%', '20.0%', '30.0%', '40.0%', '50.0%', '60.0%', '70.0%', '80.0%', '90.0%', '100.0%']
    # for each query
    for q in queries:
        # a list to record time spent
        time_list = []
        path_to_input_file = 'input/Q' + str(q) + '.json'
        query = processInputFile(path_to_input_file)
        # file to store current query's time taken data, delete if has previous records
        time_output_file_name = 'output/' + 'Time_List_' + mode + '_Q' + str(q) + '.txt'
        if os.path.exists(time_output_file_name):
            os.remove(time_output_file_name)
        # for each dataset size
        for i in range(len(dataset_size_list)):
            if randomSample:
                dataset_name = 'tpch_' + dataset_size_list[i] + '_rand'
                # print(dataset_name)
            else:
                dataset_name = 'tpch_' + dataset_size_list[i]
            query = setQueryTableName(query=query, table_name=dataset_name)
            # print(query['table'])
            # run direct and record running time
            # cumulative time, read csv as dataframe + solve
            if directMode:
                start = timeit.default_timer()
                df = getDataframe(query)
                direct(query=query, dataframe=df)
                stop = timeit.default_timer()
            # TODO sketch refine
            else:
                start = timeit.default_timer()
                df = getDataframe(query)
                direct(query=query, dataframe=df)
                stop = timeit.default_timer()
            time_taken = stop - start
            print('Current mode: %s. Time taken for Q%d with size %s: %f seconds'
                  % (mode, q, dataset_size_list[i], time_taken))
            time_list.append(time_taken)
            # write the time taken for running current query with different dataset size to a file
            with open(time_output_file_name, 'a') as f:
                f.write("%s: %s\n" % (dataset_size_list[i], str(time_taken)))

# default mode: direct; mode = sketch refine if (direct == false)
def readTimeDataAsList(queries=[2], mode='Direct', partition_core=''):
    # read time data from output files file
    all_queries_time_list = []
    for q in queries:
        # data file path
        time_data_path = 'output/' + 'Time_' + mode + partition_core \
                        + '_Q' + str(q) + '.txt'
        if not os.path.isfile(time_data_path):
            raise Exception(mode + " time data file is not a file")
        # Direct time list
        time_list = []
        with open(time_data_path, 'r') as f_direct:
            for line in f_direct:
                time = float(line)
                if time == -1:
                    time = None
                time_list.append(time)
        all_queries_time_list.append(time_list)
    return all_queries_time_list

def readPartitionSizeDataAsList(queries=[2], partition_core=''):
    # read time data from output files file
    all_queries_time_list = []
    for q in queries:
        # data file path
        time_data_path = 'output/fig8_Q' + str(q) + partition_core + '.txt'
        # print(time_data_path)
        if not os.path.isfile(time_data_path):
            raise Exception("partition size data file is not a file")
        # Direct time list
        time_list = []
        with open(time_data_path, 'r') as f_direct:
            for line in f_direct:
                time = float(line)
                if time == -1:
                    time = None
                time_list.append(time)
        all_queries_time_list.append(time_list)
    return all_queries_time_list

# figure 7
def plot_direct_vs_refine(direct_time_lists, sketch_refine_time_lists, queries=[3], partition_core='_Gaussian'):

    # plot 1 query
    if len(queries) == 1:
        # x-axis
        xs = np.arange(10, 101, 10)
        # convert to np array and apply mask to avoid invalid data (-1 denotes infeasible)
        np_direct_list = np.array(direct_time_lists[0]).astype(np.double)
        s1mask = np.isfinite(np_direct_list)
        np_sr_list = np.array(sketch_refine_time_lists[0]).astype(np.double)
        s2mask = np.isfinite(np_sr_list)
        # plot
        plt.plot(xs[s1mask], np_direct_list[s1mask], linestyle='--', marker='x', color='r')
        plt.plot(xs[s2mask], np_sr_list[s2mask], linestyle='-', marker='o', color='k')
        plt.title('Q' + str(queries[0]))
        plt.legend(['Direct', 'Sketch Refine'])
        plt.yscale('log')
        plt.xlabel('Partition size threshold')
        plt.ylabel('Running Time(s)')
        plt.savefig('output/EffectSizeThresholdQ' + str(queries[0]) + partition_core + '.png')
        plt.show()
    else:
        # plot multiple queires
        fig, axs = plt.subplots(1, len(queries), figsize=(len(queries) * 5, 7))
        for i, q in enumerate(queries):
            # x axis
            xs = np.arange(10, 101, 10)
            # convert to np array to avoid invalid data (-1 denotes infeasible)
            np_direct_list = np.array(direct_time_lists[i]).astype(np.double)
            s1mask = np.isfinite(np_direct_list)
            np_sr_list = np.array(sketch_refine_time_lists[i]).astype(np.double)
            s2mask = np.isfinite(np_sr_list)
            # plot
            axs[i].plot(xs[s1mask], np_direct_list[s1mask], linestyle='--', marker='x', color='r')
            axs[i].plot(xs[s2mask], np_sr_list[s2mask], linestyle='-', marker='o', color='k')
            axs[i].set_title('Q' + str(q))
            mean_ratio, median_ratio = computeApproximationRatio(q=q, partition_core=partition_core)
            axs[i].set(xlabel='Dataset size(%)' + '\n Approximation Ratio: \n' + 'Mean: ' + mean_ratio + ', Median: ' + median_ratio)
            #  + '\n Approximation Ratio: \n' + 'Mean: ' + mean_ratio + ', Median: ' + median_ratio
            # plt.xlabel('Dataset size')
            # plt.ylabel('Running Time(s)')
            axs[i].legend(['Direct', 'Sketch Refine'])
            axs[i].set_yscale('log')

        for ax in axs.flat:
            ax.set(ylabel='Running Time(s)')
            # ax.set(xlabel='Dataset size(%)', ylabel='Running Time(s)')
        # Hide x labels and tick labels for top plots and y ticks for right plots.
        # for ax in axs.flat:
        #     ax.label_outer()
        fig.suptitle('Scalability on TPC-H' + partition_core + 'Groups')
        plt.savefig('output/direct_vs_sketchRefine' + partition_core + '.png')
        plt.show()

# figure 8
def plot_partition_size(direct_time_lists, sketch_refine_time_lists, queries=[3], partition_core='_Gaussian'):

    # plot 1 query
    if len(queries) == 1:
        # x-axis
        # partition size
        xs = np.array([200, 400, 600, 800])
        # convert to np array and apply mask to avoid invalid data (-1 denotes infeasible)
        np_direct_list = np.array(direct_time_lists).astype(np.double)
        s1mask = np.isfinite(np_direct_list)
        np_sr_list = np.array(sketch_refine_time_lists[0]).astype(np.double)
        s2mask = np.isfinite(np_sr_list)
        # plot
        plt.plot(xs[s1mask], np_direct_list[s1mask], linestyle='--', color='r')
        plt.plot(xs[s2mask], np_sr_list[s2mask], linestyle='-', marker='o', color='k')
        plt.title('Q' + str(queries[0]))
        plt.legend(['Direct', 'Sketch Refine'])
        plt.yscale('log')
        plt.xlabel('Partition Size')
        plt.ylabel('Running Time(s)')
        plt.savefig('output/partition_size_Q' + str(queries[0]) + partition_core + '.png')
        plt.show()
    else:
        # plot multiple queires
        fig, axs = plt.subplots(1, len(queries), figsize=(len(queries) * 5, 7))
        for i, q in enumerate(queries):
            # x axis
            xs = np.array([200, 400, 600, 800])
            # convert to np array to avoid invalid data (-1 denotes infeasible)
            np_direct_list = np.array(direct_time_lists[i]).astype(np.double)
            s1mask = np.isfinite(np_direct_list)
            np_sr_list = np.array(sketch_refine_time_lists[i]).astype(np.double)
            s2mask = np.isfinite(np_sr_list)
            # plot
            axs[i].plot(xs[s1mask], np_direct_list[s1mask], linestyle='--', color='r')
            axs[i].plot(xs[s2mask], np_sr_list[s2mask], linestyle='-', marker='o', color='k')

            axs[i].set_title('Q' + str(q))
            mean_ratio, median_ratio = computeApproximationRatioFig8(q=q, partition_core=partition_core)
            axs[i].set(xlabel='Number of Groups' + '\n Approximation Ratio: \n' + 'Mean: ' + mean_ratio + ', Median: ' + median_ratio)
            print(mean_ratio, median_ratio)
            # plt.xlabel('Dataset size')
            # plt.ylabel('Running Time(s)')
            axs[i].legend(['Direct', 'Sketch Refine'])
            axs[i].set_yscale('log')

        for ax in axs.flat:
            # ax.set(xlabel='Number of Groups', ylabel='Running Time(s)')
            ax.set(ylabel='Running Time(s)')
        # Hide x labels and tick labels for top plots and y ticks for right plots.
        # for ax in axs.flat:
        #     ax.label_outer()
        fig.suptitle('Effect of number of partition groups on TPC-H' + partition_core)
        plt.savefig('output/partition_size' + partition_core + '.png')
        plt.show()

def computeApproximationRatio(q=1, partition_core='_Gaussian'):
    direct_obj_result = []
    sr_obj_result = []
    for size in range(1, 11):
        # tpch_60.0_rand_Kmeans600_Q1_SR.csv
        # tpch_60.0_rand__Q1_D.csv
        direct_file_path = 'output/tpch_' + str(size * 10) + '.0%_rand__' + 'Q' + str(q) + '_D.csv'
        sr_file_path = 'output/tpch_' + str(size * 10) + '.0%_rand' + partition_core + '_Q' + str(q) + '_SR.csv'
        if size == 10:
            direct_file_path = 'output/tpch__' + 'Q' + str(q) + '_D.csv'
            sr_file_path = 'output/tpch' + partition_core + '_Q' + str(q) + '_SR.csv'
        if not os.path.isfile(sr_file_path):
            print("can't find " + sr_file_path + ' , maybe no result(infeasible)')
            return '--', '--'
        if not os.path.isfile(direct_file_path):
            print("can't find " + direct_file_path + ' , maybe no result(infeasible)')
            return '--', '--'
        df_direct = pd.read_csv(direct_file_path)
        df_sr = pd.read_csv(sr_file_path)
        query = processInputFile('input/Q' + str(q) + '.json')
        obj = query['A0']
        if obj == 'None':
            count_direct = len(df_direct.index)
            count_sr = len(df_sr.index)
            direct_obj_result.append(count_direct)
            sr_obj_result.append(count_sr)
        else:
            direct_obj_list = df_direct[obj].tolist()
            sr_obj_list = df_sr[obj].tolist()
            # print(direct_obj_list)
            #
            # print(sr_obj_list)
            # print(direct_obj_list)
            # print(sr_obj_list)
            # append to direct result list

            direct_obj_result.append(np.sum(np.array(direct_obj_list)))
            # append to sr result list
            sr_obj_result.append(np.sum(np.array(sr_obj_list)))
    # convert to np array to compute mean/median
    direct_obj_result = np.array(direct_obj_result)
    sr_obj_result = np.array(sr_obj_result)
    ratios = direct_obj_result / sr_obj_result
    # mean
    mean = np.mean(ratios)
    # median
    median = np.median(ratios)
    # ratio

    return str(round(mean, 2)), str(round(median, 2))

def computeApproximationRatioFig8(q=1, partition_core='_Gaussian'):
    direct_obj_result = []
    sr_obj_result = []
    for size in [200, 400, 600, 800]:
        # tpch_50.0%_rand_Gaussian200_Q1_SR.csv
        # tpch_60.0_rand__Q1_D.csv
        direct_file_path = 'output/tpch_50.0%_rand__' + 'Q' + str(q) + '_D.csv'
        sr_file_path = 'output/tpch_50.0%_rand' + partition_core + str(size) + '_Q' + str(q) + '_SR.csv'
        if not os.path.isfile(sr_file_path):
            print("can't find " + sr_file_path + ' , maybe no result(infeasible)')
            return '--', '--'
        if not os.path.isfile(direct_file_path):
            print("can't find " + direct_file_path + ' , maybe no result(infeasible)')
            return '--', '--'
        df_direct = pd.read_csv(direct_file_path)
        df_sr = pd.read_csv(sr_file_path)
        query = processInputFile('input/Q' + str(q) + '.json')
        obj = query['A0']
        if obj == 'None':
            count_direct = len(df_direct.index)
            count_sr = len(df_sr.index)
            direct_obj_result.append(count_direct)
            sr_obj_result.append(count_sr)
        else:
            direct_obj_list = df_direct[obj].tolist()
            sr_obj_list = df_sr[obj].tolist()
            # print(direct_obj_list)
            #
            # print(sr_obj_list)
            # print(direct_obj_list)
            # print(sr_obj_list)
            # append to direct result list

            direct_obj_result.append(np.sum(np.array(direct_obj_list)))
            # append to sr result list
            sr_obj_result.append(np.sum(np.array(sr_obj_list)))
    # convert to np array to compute mean/median
    direct_obj_result = np.array(direct_obj_result)
    sr_obj_result = np.array(sr_obj_result)
    ratios = direct_obj_result / sr_obj_result
    # mean
    mean = np.mean(ratios)
    # median
    median = np.median(ratios)
    # ratio

    return str(round(mean, 2)), str(round(median, 2))

if __name__ == "__main__":
    # dataset sizes
    size = [x / 10 for x in range(1, 11)]
    # queries(query) to run
    queries = [1, 2, 3, 4]
    # 1. split dataset
    # random sampling, 10% - 100%
    # splitDataset(size=size, randomSample=True)
    # 2. run Direct and record time on Q1 - Q4, with dataset size 10% - 100%
    # computeTimeCost(queries=queries, size=size, randomSample=True)
    # 3. run sketchRefine and record time

    # 4. fig 7,  read time data as list
    direct_time_list = readTimeDataAsList(queries=queries, mode='Direct', partition_core='')
    sketch_refine_time_k_means_list = readTimeDataAsList(queries=queries, mode='SketchRefine', partition_core='_Kmeans600')
    sketch_refine_time_gaussian_list = readTimeDataAsList(queries=queries, mode='SketchRefine', partition_core='_Gaussian600')

    # 5. plot  figure 7
    plot_direct_vs_refine(direct_time_lists=direct_time_list,
                          sketch_refine_time_lists=sketch_refine_time_k_means_list,
                          queries=queries, partition_core='_Kmeans600')

    plot_direct_vs_refine(direct_time_lists=direct_time_list,
                          sketch_refine_time_lists=sketch_refine_time_gaussian_list,
                          queries=queries, partition_core='_Gaussian600')


    # 6. plot figure 8

    # read time data
    # hard code, obtained from time_direct_q1-4
    direct_time_list_par = [[466.1278363, 466.1278363, 466.1278363, 466.1278363],
                        [156.06291409999994, 156.06291409999994, 156.06291409999994, 156.06291409999994],
                        [168.53227549999974, 168.53227549999974, 168.53227549999974, 168.53227549999974],
                        [179.41443570000047, 179.41443570000047, 179.41443570000047, 179.41443570000047]]
    k_means_partition_time_list = readPartitionSizeDataAsList(queries=queries, partition_core='_Gaussian')
    gaussian_partition_time_list = readPartitionSizeDataAsList(queries=queries, partition_core='_Kmeans')

    # plot
    plot_partition_size(direct_time_lists=direct_time_list_par,
                        sketch_refine_time_lists=k_means_partition_time_list, queries=queries, partition_core='_Kmeans')
    plot_partition_size(direct_time_lists=direct_time_list_par,
                        sketch_refine_time_lists=gaussian_partition_time_list, queries=queries,
                        partition_core='_Gaussian')
    # print(k_means_partition_time_list)
    # print(gaussian_partition_time_list)

