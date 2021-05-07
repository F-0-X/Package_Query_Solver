import argparse
from src.paql import main
from src.utils import *

def run_more_helper(args):
    # For all Query
    for i in range(1, 5):
        file_name = 'Q' + str(i) + '.json'
        path_to_input_file = args.input_dir + file_name
        querybase = processInputFile(path_to_input_file)
        # With all kinds of dataset size
        time_list = []
        args.input_file = file_name
        for j in range(1, 11):
            dataset_name = 'tpch'
            postfix = ''
            if j != 10:
                postfix = '_' + str(j) + '0.0%_rand'
            dataset_file = dataset_name + postfix
            querybase['table'] = dataset_file

            time_taken = main(args, querybase)
            time_list.append(time_taken)

            if args.extension:
                partition_core = '_Gaussian'
            else:
                partition_core = '_Kmeans'

            if args.advance:
                mode = 'SketchRefine'
            else:
                mode = 'Direct'
                partition_core = ''

            file_name = 'output/' + 'Time_' + mode + partition_core \
                        + '_Q' + str(i) + '.txt'
            with open(file_name, 'a') as f:
                f.write("%s\n" % str(time_taken))
            print('Q' + str(i))
            print(time_list)

def run_more(args):

    # We keep using cplex in this evaluation
    args.use_cplex = True

    # Sketch Refine
    args.advance = True
    # use which kind of partition core
    # args.extension = False
    run_more_helper(args)



    # Direct
    # args.advance = False

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='paql')

    parser.add_argument("-a", "--advance", action="store_true",
                        help="Advance mode - Use SketchRefine instead of Direct")

    parser.add_argument("-c", "--use_cplex", action="store_true", default=False, help="choose to use cplex solver")

    parser.add_argument("-e", "--extension", action="store_true", default=False,
                        help="use GaussianParitionCore (default partition core is KmeansPartitionCore)")

    parser.add_argument("--num_groups", default=400, type=int, help="number of groups in SketchRefine")

    # argument for default read in data file address
    parser.add_argument("--input_dir", default="./input/", type=str, help="folder for input json file")
    # argument for default input(json file) address
    parser.add_argument("--input_file", default="Q1.json", type=str, help="input file name")

    parser.add_argument("--output_dir", default="output/", type=str, help="result folder")

    # add argument for default dataset folder
    parser.add_argument("--data_dir", default="data/", type=str, help="folder storing datasets")
    parser.add_argument("--temp_dir", default="temp/", type=str,
                        help="folder storing temp datasets like partitions or sketch result")

    run_more(parser.parse_args())
