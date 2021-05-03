import json
import os
import pandas as pd
import matplotlib.pyplot as plt

def processInputFile(path_to_input_file):
    if not os.path.isfile(path_to_input_file):
        raise Exception("input file is not a file")

    data = None
    with open(path_to_input_file) as json_file:
        data = json.load(json_file)
    if len(data['A']) != len(data['L']) or len(data['A']) != len(data['U']):
        raise Exception('invalid input file')
    return data

def setQueryTableName(query, table_name='tpch'):
    query['table'] = table_name
    return query

def splitDataset(size=[0.1, 0.4, 0.7], path_to_input_file='../data/tpch.csv', randomSample=False):
    if not os.path.isfile(path_to_input_file):
        raise Exception("input file is not a file")

    # read dataset as dataframe
    dataset = pd.read_csv(path_to_input_file)
    # total number of rows in the dataset, or dataset.shape[0]
    row_cnt = len(dataset.index)  # 6001215 header not included

    for percent in size:
        file_path_name = '../data/tpch_' + str(percent * 100) + '%'
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
    plt.savefig('../output/direct_vs_sketchRefine.png')
    # plt.show()

def plotDirect(dataset_size, direct_time_taken, query_name, x_label, y_label):
    dataset_size = [x / 10 for x in range(1, 11)]
    plt.plot(dataset_size, direct_time_taken)
    plt.title(query_name)
    plt.xlabel('Dataset size')
    plt.ylabel('Time')
    plt.legend(['Direct'])
    plt.savefig('../output/direct_' + query_name + '.png')


# TODO a helper function which can help us generate the name of partition file