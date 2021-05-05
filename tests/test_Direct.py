from unittest import TestCase
from src.Direct import *
from src.utils import *


class TestDirect(TestCase):

    def test_2(self):
        path_to_dataset = "data/"

        path_to_input_file = "input/Q2.json"
        query = processInputFile(path_to_input_file)

        direct(query, path_to_dataset)

    def testDirect(queries=[2]):
        path_to_dataset = "data/"

        path_to_input_file = "input/Q2.json"
        query = processInputFile(path_to_input_file)

        dataset_size_list = [str(int(i * 10)) + '.0%' for i in range(1, 11)]
        # print(dataset_size_list)
        # ['10.0%', '20.0%', '30.0%', '40.0%', '50.0%', '60.0%', '70.0%', '80.0%', '90.0%', '100.0%']
        # for each query
        for q in queries:
            time_list = []
            path_to_input_file = 'input/Q' + str(q) + '.json'
            query = processInputFile(path_to_input_file)
            # for each dataset size
            for size in range(len(dataset_size_list)):
                dataset_name = 'tpch_' + dataset_size_list[size]
                query = setQueryTableName(query=query, table_name=dataset_name)
                df = getDataframe(query)
                print(query['table'])
                # run direct and record running time
                start = timeit.default_timer()
                direct(query=query, dataframe=df)
                stop = timeit.default_timer()
                time_taken = stop - start
                print('Time taken for Q', q, ' with size ', dataset_size_list[size], ': ', time_taken)
                time_list.append(time_taken)
                # write the time taken for running current query with different dataset size to a file
                file_name = 'output/' + 'Time_List_Direct_Q' + str(q) + '.txt'
                with open(file_name, 'a') as f:
                    f.write("%s\n" % str(time_taken))
            # plot
            # plotDirect(direct_time_taken=time_list, query_name='Q' + str(q))