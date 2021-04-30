from utils import *
import pulp
import pandas as pd
import numpy as np

def direct(query, data_dir):
    table_name = query["table"]
    path_to_dataset = data_dir + table_name + '.csv'

    if not os.path.isfile(path_to_dataset):
        # TODO maybe we can choose to return empty query result
        raise Exception("can't find the table in the query")
    Table = pd.read_csv(path_to_dataset, sep=',')

    # defind min or max problem
    prob = pulp.LpProblem("Package Query", pulp.LpMaximize)
    if query["max"] is False:
        prob = pulp.LpProblem("Beer Distribution Problem", pulp.LpMinimize)

    # generate variables and add objective function
    A_zero = query["A0"]
    vars = np.arange(1, len(Table)+1)
    if A_zero == "None":
        prob += pulp.lpSum(vars)
    else:
        A_col = Table[[A_zero]].to_numpy()
        prob += pulp.lpSum(np.dot(A_col, vars))

    # add constraint functions
    A_cons = query["A"]

    # TODO transform the argument to something that ILP solver can understand (might move to util later)
    # TODO use the PuLP to solve the ILP problem considering the database


def var_generator(num):
    return pulp.LpVariable("x" + str(num), cat='Binary')
