from queue import Queue

from src.Direct import direct
from src.utils import OptimizeObjective, GroupAndRepresentationTuple, SimplePQ
import random
import pandas as pd


class SketchRefine:

    def __init__(self):
        # updated by sketch and used by refine
        self.P = None

    def sketch(self, query, load_write_helper):
        #  we firstly build a representation table, where each tuple represent a groups
        #  To ensure approximation guarantees (Section 5.1),
        #  the maximum (minimum, resp.) value is chosen for a maximization (minimization, resp.) query.
        #  For all other attributes, the algorithm picks the average value.
        table_name = query["table"]
        objective = OptimizeObjective.MAXIMIZE
        if not query['max']:
            objective = OptimizeObjective.MINIMIZE
        rep_df = load_write_helper.getReprecentation(table_name, objective)

        # TODO secondly, we want to use Direct to solve the query with the representation table as input
        #  (We need to add some new global constraints to ensure that every representative tuple
        #  does not appear in sketch result more times than the size of its group

        df = pd.read_csv('temp/tpch10_representative.csv')
        # get average value for A1...Ak
        rep_mean = df.loc[df['rpst'] == 'MEAN']

        # if A0 is not none
        if query['A0'] != 'None':
            if query['max']:
                rep_obj = df.loc[df['rpst'] == 'MAX']
            else:
                rep_obj = df.loc[df['rpst'] == 'MIN']
            # replace A0 column
            # TODO May need to add order by gid to ensure that the replacement is in order?
            rep_mean[query['A0']] = rep_obj[query['A0']].values
        # rep_mean is the representation table

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

        # TODO we need to update the self.P, which is a set of utils.GroupAndRepresentationTuple

        # TODO we finally return ps, which is a dataframe looks like this
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

        return 1

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
            curr_representation_tuple = ps[(ps['group_id'] == curr_group_id) & (ps['refined'] == False)]
            if curr_representation_tuple.empty:
                continue

            pi = direct(Q(curr_group, ps))

            if pi is not None:
                ps_next = ps[(ps['group_id'] != curr_group_id) & (ps['refined'] == True)].copy()
                ps_next = ps_next.append(pi)
                S.remove(curr_group)

                p, failed_groups_new = self.refine(Q, S, ps_next)
                S.add(curr_group)

                if failed_groups_new is not None and not failed_groups_new.empty():
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

        # TODO check failed_group == S
        return None, failed_groups

    def sketch_and_refine(self, query, load_write_helper):
        # TODO take all input, call sketch, update class variable
        #  call refine and return the result package

        def q(target, ps):
            if query['A0'] != 'None':
                all_count = ps[query['A0']].sum()
                others_count = all_count - target.get_num_of_tuple()
                if query['Lc'] != 'None':
                    query['Lc'] -= others_count
                if query['Uc'] != 'None':
                    query['Uc'] -= others_count

            for i, Ai in enumerate(query['A']):
                all_sum = int((ps[Ai] * ps['num_of_tuple']).sum())
                others_sum = all_sum - int(target.get_representation_tuple()[Ai][1]) * target.get_num_of_tuple()
                if query['L'][i] != 'None':
                    query['L'][i] -= others_sum
                if query['U'][i] != 'None':
                    query['U'][i] -= others_sum

            return query, target.get_group_df()

        sketch_result = self.sketch(query, load_write_helper)
        return self.refine(q, self.P.copy(), sketch_result)
