import json
import os
from enum import Enum


def processInputFile(path_to_input_file):
    if not os.path.isfile(path_to_input_file):
        raise Exception("input file is not a file")

    data = None
    with open(path_to_input_file) as json_file:
        data = json.load(json_file)
    if len(data['A']) != len(data['L']) or len(data['A']) != len(data['U']):
        raise Exception('invalid input file')
    return data

class OptimizeObjective(Enum):
    MAXIMIZE = 1
    MINIMIZE = 2
    AVERAGE = 3

class LoadAndWrite:

    def __init__(self, args):
        self.partition_dir = args.temp_dir

    # This method is used by sketch to get the representation table
    def getReprecentation(table_name, objective = OptimizeObjective.MAXIMIZE):

        # TODO get path to file by table name and self.partition_dir

        # TODO read in corresponding csv

        # TODO filter the df to get the representation table and return it
        a = 1

# TODO a helper function which can help us generate the name of partition file


