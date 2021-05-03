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
    if query["max"] is False:
        prob = pulp.LpProblem("Package_Query_Minimize", pulp.LpMinimize)
    else:
        prob = pulp.LpProblem("Package_Query_Maximize", pulp.LpMaximize)

    # generate variables and add objective function
    A_zero = query["A0"]
    vars = Table[["id"]].to_numpy().reshape(-1)
    vars_dic = pulp.LpVariable.dicts("x", vars, cat='Binary')

    # vars_vector = np.vectorize(var_generator)
    # vars = vars_vector(vars)
    vars = np.array(list(vars_dic.values()))

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

    sketch = False
    # sketch = True
    gid = 2  # or number of clusters
    if sketch:
        for index in range(gid + 1):
            gID = Table.loc[Table['gid'] == index]
            idx = gID[["id"]].to_numpy().reshape(-1)
            vars_name = ['x_' + str(i) for i in idx]
            var = [vars_dic[vname] for vname in vars_name]
            var = np.array(var)
            prob += pulp.lpSum(var) <= len(idx)


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




    if prob.status:
        result = []
        for v in prob.variables():
            result.append([v.name, v.varValue])
        return result
    else:
        return None


def var_generator(num):
    return pulp.LpVariable("x" + str(num), cat='Binary')
