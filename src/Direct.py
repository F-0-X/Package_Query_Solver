# from utils import *
from pulp import *
import pandas as pd
import numpy as np

# TODO This method should take dataframe instead of data_dir
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
        A_zero_col = Table[[A_zero]].to_numpy()
        prob += pulp.lpSum(np.dot(A_zero_col, vars))

    # add constraint functions
    A_cons = query["A"]
    for i, constrains in enumerate(A_cons):
        cons_col = Table[[constrains]].to_numpy()
        lower_bound = query["L"]
        if lower_bound !=  "None":
            a = 1
        upper_bound = query["U"]
        prob += pulp.lpSum(np.dot(cons_col, vars))



    # TODO transform the argument to something that ILP solver can understand (might move to util later)
    # TODO use the PuLP to solve the ILP problem considering the database