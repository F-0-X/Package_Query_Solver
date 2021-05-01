import json
import os

def processInputFile(path_to_input_file):
    if not os.path.isfile(path_to_input_file):
        raise Exception("input file is not a file")

    data = None
    with open(path_to_input_file) as json_file:
        data = json.load(json_file)
    if len(data['A']) != len(data['L']) or len(data['A']) != len(data['U']):
        raise Exception('invalid input file')
    return data

# TODO a helper function which can help us generate the name of partition file