from utils import *
from pulp import *
import pandas as pd

def direct(query, data_dir):

    table_name = query["table"]
    path_to_dataset = data_dir + table_name + '.csv'

    if not os.path.isfile(path_to_dataset):
        # TODO maybe we can choose to return empty query result
        raise Exception("can't find the table in the query")
    df = pd.read_csv(path_to_dataset, sep=',')
    value = df.values



    # TODO transform the argument to something that ILP solver can understand (might move to util later)
    # TODO use the PuLP to solve the ILP problem considering the database