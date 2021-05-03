from queue import Queue

from src.utils import OptimizeObjective, GroupAndRepresentationTuple, SimplePQ
import random


class SketchRefine:

    def __init__(self):
        #
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
            objective = OptimizeObjective.MAXIMIZE
        rep_df = load_write_helper.getReprecentation(table_name, objective)

        # TODO secondly, we want to use Direct to solve the query with the representation table as input
        #  (We need to add some new global constraints to ensure that every representative tuple
        #  does not appear in sketch result more times than the size of its group

        # TODO we need to update the self.P, which is a set of utils.GroupAndRepresentationTuple

        # TODO we finally return ps, which is a list of (we will have a new class for this)

        a = 1

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
            if curr_group not in ps:
                continue



        return (None, failed_groups)

    def sketch_and_refine(self, query, load_write_helper):
        # TODO take all input, call sketch, update class variable
        #  call refine and return the result package

        self.sketch(query, load_write_helper)


