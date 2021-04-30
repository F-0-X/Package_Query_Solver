import argparse
import os

from Direct import *
from utils import *
from partition import *


def main(args):
    print(args)
    # TODO parse the args into variables
    path_to_input_file = os.path.join(args.input_dir, args.input_file)
    query = processInputFile(path_to_input_file)

    if args.advance:
        print('SketchRefine Mode')
        # TODO add code for SketchRefine here
        partition(args.size_threshold, args.diameter_bound, path_to_input_file)
    else:
        print('Direct Mode')
        # TODO add code for Direct here
        direct(query, args.data_dir)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='pqql')

    parser.add_argument("-a", "--advance", action="store_true",
                        help="Advance mode - Use SketchRefine instead of Direct")

    # argument for default read in data file address
    parser.add_argument("--input_dir", default="./input/", type=str, help="folder for input json file")
    # argument for default input(json file) address
    parser.add_argument("--input_file", default="Q2.json", type=str, help="input file name")

    # TODO add argument for default output directory and file name

    # add argument for default dataset folder
    parser.add_argument("--data_dir", default="data/", type=str, help="folder storing datasets")

    # add argument for partition size threshold and diameter bound
    parser.add_argument("--size_threshold", default=100, type=int,
                        help="the size threshold, geq 1 and leq n, restricts the size of each partition group")
    parser.add_argument("--diameter_bound", default=10, type=float,
                        help="the size threshold, geq 1 and leq n, restricts the size of each partition group")

    # print(parser.parse_args())
    main(parser.parse_args())