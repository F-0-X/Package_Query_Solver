import os
import pulp
import pandas as pd
import numpy as np
import timeit

def direct(query, data_dir):

    table_name = query["table"]
    path_to_dataset = data_dir + table_name + '.csv'

    if not os.path.isfile(path_to_dataset):
        # TODO maybe we can choose to return empty query result
        raise Exception("can't find the table in the query")
    Table = pd.read_csv(path_to_dataset, sep=',')

    # defind min or max problem

    prob = pulp.LpProblem("Package_Query_Maximize", pulp.LpMaximize)
    if query["max"] is False:
        prob = pulp.LpProblem("Package_Query_Minimize", pulp.LpMinimize)

    # generate variables and add objective function
    A_zero = query["A0"]
    vars = np.arange(1, len(Table) + 1)
    vars_vector = np.vectorize(var_generator)
    vars = vars_vector(vars)
    if A_zero == "None":
        prob += pulp.lpSum(vars)
    else:
        A_zero_col = Table[[A_zero]].to_numpy().reshape(-1)
        # do not use np.dot, really slow
        # prob += pulp.lpSum( np.dot(A_zero_col, vars))
        prob += pulp.lpSum(A_zero_col * vars)


    # Lc and Uc constrains
    lower_count = query["Lc"]
    upper_count = query["Uc"]
    if lower_count != 'None':
        prob += pulp.lpSum(vars) >= int(lower_count)
    if upper_count != 'None':
        prob += pulp.lpSum(vars) <= int(upper_count)

    # add constraint functions
    A_cons = query["A"]
    for i, constrains in enumerate(A_cons):
        cons_var = vars
        if constrains != 'None':
            cons_col = Table[[constrains]].to_numpy().reshape(-1)
            cons_var = cons_col * vars # np.dot(cons_col, vars)
        lower_bound = query["L"][i]
        if lower_bound != 'None':
            prob += pulp.lpSum(cons_var) >= lower_bound
        upper_bound = query["U"][i]
        if upper_bound != 'None':
            prob += pulp.lpSum(cons_var) <= upper_bound

    prob.solve()
    print("Status:", pulp.LpStatus[prob.status])
    # for v in prob.variables():
    #     print(v.name, "=", v.varValue)

def directForLoop(query, data_dir):

    table_name = query["table"]
    path_to_dataset = data_dir + table_name + '.csv'

    if not os.path.isfile(path_to_dataset):
        # TODO maybe we can choose to return empty query result
        raise Exception("can't find the table in the query")
    Table = pd.read_csv(path_to_dataset, sep=',')

    # defind min or max problem

    prob = pulp.LpProblem("Package_Query_Maximize", pulp.LpMaximize)
    if query["max"] is False:
        prob = pulp.LpProblem("Package_Query_Minimize", pulp.LpMinimize)

    A_zero = query['A0']

    # generate variables
    variables = []
    for i in range(len(Table.index)):
        variables.append(var_generator(i + 1))
    # if A_zero == 'None':
          # count





def var_generator(num):
    return pulp.LpVariable("x" + str(num), cat='Binary')
