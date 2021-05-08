import argparse

from src.partition import *
import multiprocessing

from src.utils import LoadAndWrite


def partition_one(num_of_groups, table_name, load_write_helper, extension):
    if extension:
        partition_core = GaussianParitionCore(num_of_groups)
    else:
        partition_core = KmeansPartitionCore(num_of_groups)

    partition(partition_core, table_name, load_write_helper)



def partition_all(args):

    partition_fig7(args)
    partition_fig8(args)


def partition_fig7(args):
    table_options = ['tpch_10.0%_rand', 'tpch_20.0%_rand', 'tpch_30.0%_rand', 'tpch_40.0%_rand', 'tpch_50.0%_rand',
                     'tpch_60.0%_rand', 'tpch_70.0%_rand', 'tpch_80.0%_rand', 'tpch_90.0%_rand', 'tpch']
    table_options.reverse()
    extension_options = [True, False]
    for t in table_options:
        for e in extension_options:
            partition_one(600, t, LoadAndWrite(args), e)
        print("fig7" + t + " Done")


def partition_fig8(args):

    group_num_options = [200, 600, 800]
    extension_options = [True, False]

    tasks = []
    for gn in group_num_options:
        for e in extension_options:
            partition_one(gn, 'tpch_50.0%_rand', LoadAndWrite(args), e)


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

    partition_all(parser.parse_args())
