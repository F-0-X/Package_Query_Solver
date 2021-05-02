from src.utils import OptimizeObjective


class SketchRefine:

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

        a = 1

    # Note that this is a recursive function, and it relay on some class variable
    def refine(self, Q, P, ps):
        # TODO follow the Pseudocode in the paper
        a = 1

    def sketch_and_refine(self, query, load_write_helper):
        # TODO take all input, call sketch, update class variable
        #  call refine and return the result package

        self.sketch(query, load_write_helper)
