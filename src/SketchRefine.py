import copy
from queue import Queue

from src.Direct import direct
from src.utils import OptimizeObjective, GroupAndRepresentationTuple, SimplePQ
import random
import pandas as pd
import numpy as np


class SketchRefine:

    def __init__(self):
        # updated by sketch and used by refine
        self.P = None

    def sketch(self, query, rep_df):
        #  we firstly build a representation table, where each tuple represent a groups
        #  To ensure approximation guarantees (Section 5.1),
        #  the maximum (minimum, resp.) value is chosen for a maximization (minimization, resp.) query.
        #  For all other attributes, the algorithm picks the average value.
        table_name = query["table"]
        objective = OptimizeObjective.MAXIMIZE
        if not query['max']:
            objective = OptimizeObjective.MINIMIZE


        # TODO secondly, we want to use Direct to solve the query with the representation table as input
        #  (We need to add some new global constraints to ensure that every representative tuple
        #  does not appear in sketch result more times than the size of its group

        # df = pd.read_csv('temp/tpch10_representative.csv')
        df = rep_df
        # get average value for A1...Ak
        rep_mean = df.loc[df['rpst'] == 'MEAN']

        # if A0 is not none
        if query['A0'] != 'None':
            if query['max']:
                rep_obj = df.loc[df['rpst'] == 'MAX']
            else:
                rep_obj = df.loc[df['rpst'] == 'MIN']
            # replace A0 column
            # TODO May need to add (order by gid) to ensure that the replacement is in order?
            #  maybe raise exception when row number differs
            rep_mean[query['A0']] = rep_obj[query['A0']].values
        pd.set_option('display.max_columns', None)
        # print(rep_mean)
        # delete rpst column, no longer needed
        rep_mean.drop(columns='rpst')
        # R = representation table
        R = rep_mean.drop(columns='rpst')
        # reset index for using direct method
        R = R.reset_index(drop=True)
        """
            representation table looks like this
            +---------------+--------+
            |original fields|group id|
            +---------------+--------+
            |               |   1    |
            +---------------+--------+
            |               |   2    |
            +---------------+--------+
            |               |   3    |
            +---------------+--------+
            |               |   4    |
            +---------------+--------+
            |               |   5    |
            +---------------+--------+
        
        """
        ps = direct(query=query, dataframe=R, is_sketch=True)



        if ps is None:
            return None

        # otherwise we return a (result dataframe from direct) + (a column specifying all tuples are not refined yet)

        append_col = [False] * ps.shape[0]
        ps['refined'] = append_col
        ps = ps.drop(columns='g_size')

        # order boy gid
        # ps = ps.sort_values(by=['git'])

        # we finally return ps, which is a dataframe looks like this
        """
            +---------------+--------+-------------+-------+
            |original fields|group_id| num_of_tuple|refined|
            +---------------+--------+-------------+-------+
            |               |   1    |      3      | False |
            +---------------+--------+-------------+-------+
            |               |   2    |      0      | False |
            +---------------+--------+-------------+-------+
            |               |   3    |      2      | False |
            +---------------+--------+-------------+-------+
            |               |   4    |      1      | False |
            +---------------+--------+-------------+-------+
            |               |   5    |      5      | False |
            +---------------+--------+-------------+-------+
        
        """
        # if ps is empty (infeasible), would return None

        return ps

    # Note that this is a recursive function, and it relay on some class variable
    # Q is the package query to evaluate
    # P is the partition groups, each entry in P is tuple(Group, Group Representation)
    # S is partitioning groups yet to be refined (initially S = P)
    # ps is the refining package (initially the result of SKETCH)
    def refine(self, Q, S, ps):
        # TODO follow the Pseudocode in the paper
        failed_groups = []
        if S is None:
            raise Exception("call sketch before refine")
        if len(S) == 0:
            return ps, failed_groups

        random_S = list(S)
        random.shuffle(random_S)

        high_priority_queue = Queue()
        low_priority_queue = Queue()
        for group in random_S:
            low_priority_queue.put(group)

        pq = SimplePQ(failed_groups, high_priority_queue, low_priority_queue)

        while True:
            curr_group = pq.pop()
            if curr_group is None:
                # This means the priority queue is empty
                break
            curr_group_id = curr_group.get_group_id()
            if curr_group.get_num_of_tuple == 0:
                continue
            Q1, Q2 = Q(curr_group, ps)
            pi = direct(Q1, Q2)

            append_col = [True] * pi.shape[0]
            pi['refined'] = append_col

            if pi is not None:
                ps_next = ps[(ps['gid'] != curr_group_id) | (ps['refined'] == True)].copy()
                ps_next = ps_next.append(pi)
                S.remove(curr_group)

                p, failed_groups_new = self.refine(Q, S, ps_next)
                S.add(curr_group)

                if failed_groups_new is not None and len(failed_groups_new) > 0:
                    # refine failure
                    # greedily prioritize non-refinable groups
                    pq.prioritize(failed_groups_new)  # This method also update failed_group
                else:
                    # refine success
                    # why are we returning the failed_groups? we can just return a None
                    return p, failed_groups

            else:
                if S != self.P:
                    failed_groups.append(curr_group)
                    return None, failed_groups

        # check failed_group == S
        if failed_groups != S:
            raise Exception('Something wrong with the refine')
        return None, failed_groups

    def sketch_and_refine(self, query, load_write_helper, partition_core):
        # TODO take all input, call sketch, update class variable
        #  call refine and return the result package

        def q(target, ps):
            query_copy2 = copy.deepcopy(query)
            if query_copy2['A0'] != 'None':
                all_count = int(ps['num_of_tuple'].sum())
                others_count = all_count - target.get_num_of_tuple()
                if query_copy2['Lc'] != 'None':
                    query_copy2['Lc'] = int(query_copy2['Lc']) - others_count
                if query_copy2['Uc'] != 'None':
                    query_copy2['Uc'] = int(query_copy2['Uc']) - others_count

            for i, Ai in enumerate(query_copy2['A']):
                all_sum = int((ps[Ai] * ps['num_of_tuple']).sum())
                others_sum = all_sum - int(target.get_representation_tuple()[Ai]) * target.get_num_of_tuple()
                if query_copy2['L'][i] != 'None':
                    query_copy2['L'][i] -= others_sum
                if query_copy2['U'][i] != 'None':
                    query_copy2['U'][i] -= others_sum

                t_df = target.get_group_df()
            return query_copy2, t_df

        table_name = query["table"]
        rep_df = load_write_helper.get_reprecentation(table_name, partition_core)

        sketch_result = self.sketch(copy.deepcopy(query), rep_df)
        if sketch_result is None:
            return None

        # we need to update the self.P, which is a set of utils.GroupAndRepresentationTuple
        self.P = set()
        for i in range(sketch_result.shape[0]):
            rep_tuple = sketch_result.loc[i, :]
            group_id = int(rep_tuple['gid'])
            curr_df = load_write_helper.get_partition_group(table_name, partition_core, group_id)
            curr = GroupAndRepresentationTuple(rep_tuple, curr_df)
            self.P.add(curr)

        refine_result, refine_failed_group = self.refine(q, self.P.copy(), sketch_result)
        return refine_result