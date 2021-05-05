import os
import pulp
import pandas as pd
import numpy as np
import timeit


def direct(query, dataframe):
    # table_name = query["table"]
    # path_to_dataset = data_dir + table_name + '.csv'
    #
    # if not os.path.isfile(path_to_dataset):
    #     # TODO maybe we can choose to return empty query result
    #     raise Exception("can't find the table in the query")
    Table = dataframe
    print(query)
    print(Table)
    # defind min or max problem
    if query["max"] is False:
        prob = pulp.LpProblem("Package_Query_Minimize", pulp.LpMinimize)
    else:
        prob = pulp.LpProblem("Package_Query_Maximize", pulp.LpMaximize)

    # generate variables and add objective function
    A_zero = query["A0"]
    # vars = Table[["id"]].to_numpy().reshape(-1)
    vars = list(Table.index)
    vars_dic = pulp.LpVariable.dicts("x", vars, cat='Binary')

    gid = 2  # or number of clusters
    if 'gid' in Table.columns:
        vars_dic = pulp.LpVariable.dicts("x", vars, 0, None, cat=pulp.LpInteger)
        gid_list = Table[["gid"]].to_numpy().reshape(-1)
        gid = len(np.unique(gid_list))

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

            cons_var = cons_col * vars  # np.dot(cons_col, vars)

        lower_bound = query["L"][i]
        if lower_bound != 'None':
            prob += pulp.lpSum(cons_var) >= lower_bound
        upper_bound = query["U"][i]
        if upper_bound != 'None':
            prob += pulp.lpSum(cons_var) <= upper_bound

    # print(vars_dic)
    # if is using sketch
    if 'gid' in Table.columns and 'g_size' in Table.columns:
        for index in range(gid):
            gID = Table.loc[Table['gid'] == index]
            # idx = gID[["id"]].to_numpy().reshape(-1)
            idx = list(gID.index)
            g_id_df = gID[["g_size"]].to_numpy().reshape(-1)
            groupsize = np.amax(g_id_df)
            vars_name = [i for i in idx]
            var = [vars_dic[vname] for vname in vars_name]
            var = np.array(var)
            prob += pulp.lpSum(var) <= groupsize

    prob.solve()
    print("Status:", pulp.LpStatus[prob.status])
    # for v in prob.variables():
    #     print(v.name, "=", v.varValue)

    # if prob.status == 1:
    #     result = []
    #     for v in prob.variables():
    #         result.append([v.name, v.varValue])
    #     return result
    # else:
    #     return None

    if prob.status != 1:
        return None
    # a list to store original input tuples, maybe faster than using dataframe append/concat
    tuples_list = []
    # a list to store 'num_of_tuple'
    num_tuples_list = []
    # for each variables(rows)
    for v in prob.variables():
        # number of times the tuple we repeatedly choose, can be 0
        # for i in range(int(v.varValue)):
        tuples_list.append(dataframe.iloc[int(v.name[2:]), :])
        num_tuples_list.append(int(v.varValue))
    res_df = pd.DataFrame(tuples_list)
    res_df['num_of_tuple'] = num_tuples_list
    # return a new df:
    # +---------------+ -------- +------------- +
    # |original fields| group_id | num_of_tuple |
    # +---------------+ -------- +------------- +
    return res_df


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
