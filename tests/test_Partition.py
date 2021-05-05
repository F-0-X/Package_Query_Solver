import argparse
from unittest import TestCase
from src.partition import *
from src.utils import *


class TestPartition(TestCase):

    def setUp(self):
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
        parser.add_argument("--temp_dir", default="temp/", type=str,
                            help="folder storing temp datasets like partitions or sketch result")

        # add argument for partition size threshold and diameter bound
        parser.add_argument("--size_threshold", default=100, type=int,
                            help="the size threshold, geq 1 and leq n, restricts the size of each partition group")
        parser.add_argument("--diameter_bound", default=10, type=float,
                            help="the size threshold, geq 1 and leq n, restricts the size of each partition group")

        self.parser = parser
        self.load_write_helper = LoadAndWrite(parser.parse_args())


    def test_2(self):

        partition_core = KmeansPartitionCore(2)
        partition(partition_core, 'tpch10', self.load_write_helper)