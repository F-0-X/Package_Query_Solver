import argparse

from Direct import *
from src.SketchRefine import SketchRefine
from src.utils import *
from partition import *
import os


def main(args):
    print(args)
    # TODO parse the args into variables
    path_to_input_file = os.path.join(args.input_dir, args.input_file)
    query = processInputFile(path_to_input_file)
    load_write_helper = LoadAndWrite(args)
    query_name = os.path.splitext(args.input_file)[0]

    if args.advance:
        print('SketchRefine Mode')
        # TODO we need to call this function for all large csv in the data folder(and don't delete if forever)
        # TODO we need to skip making partition if we already do the partition

        partition_core = KmeansPartitionCore(400)
        partition(partition_core, query['table'], load_write_helper)

        worker = SketchRefine()
        sketch_result_df = worker.sketch_and_refine(query, load_write_helper, partition_core)
        load_write_helper.store_output(sketch_result_df, query['table'], query_name, partition_core=partition_core)
    else:
        print('Direct Mode')
        df = load_write_helper.load_initial_table(query['table'])
        direct_df = direct(query, df)
        load_write_helper.store_output(direct_df, query['table'], query_name, is_direct_mode=True)
        a = 1

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='paql')

    parser.add_argument("-a", "--advance", action="store_true",
                        help="Advance mode - Use SketchRefine instead of Direct")

    # argument for default read in data file address
    parser.add_argument("--input_dir", default="./input/", type=str, help="folder for input json file")
    # argument for default input(json file) address
    parser.add_argument("--input_file", default="Q3.json", type=str, help="input file name")

    parser.add_argument("--output_dir", default="output/", type=str, help="result folder")

    # add argument for default dataset folder
    parser.add_argument("--data_dir", default="data/", type=str, help="folder storing datasets")
    parser.add_argument("--temp_dir", default="temp/", type=str,
                        help="folder storing temp datasets like partitions or sketch result")

    # add argument for partition size threshold and diameter bound
    parser.add_argument("--size_threshold", default=100, type=int,
                        help="the size threshold, geq 1 and leq n, restricts the size of each partition group")
    parser.add_argument("--diameter_bound", default=10, type=float,
                        help="the size threshold, geq 1 and leq n, restricts the size of each partition group")

    # print(parser.parse_args())
    main(parser.parse_args())
