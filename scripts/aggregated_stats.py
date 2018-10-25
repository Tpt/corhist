import csv

import math
import sys


class Eval:
    def __init__(self, precision, recall, count):
        self.precision = float(precision)
        self.recall = float(recall)
        self.count = int(count)

    @property
    def f1(self):
        return (2 * self.precision * self.recall) / (self.precision + self.recall)


def weighted_average(v1, p1, v2, p2):
    if math.isnan(v1):
        return v2
    if math.isnan(v2):
        return v1
    if p1 + p2 == 0:
        return 0
    return (v1 * p1 + v2 * p2) / (p1 + p2)


def read_eval(row, prefix):
    return Eval(row[prefix + ' precision'], row[prefix + ' recall'], row['test set size'])


def weighted_combination(e1: Eval, e2: Eval):
    return Eval(
        weighted_average(e1.precision, e1.count, e2.precision, e2.count),
        weighted_average(e1.recall, e1.count, e2.recall, e2.count),
        e1.count + e2.count
    )


def average_combination(e1: Eval, e2: Eval):
    return Eval(
        weighted_average(e1.precision, 1, e2.precision, 1),
        weighted_average(e1.recall, 1, e2.recall, 1),
        e1.count + e2.count
    )


with open(sys.argv[1], newline='') as csvfile:
    reader = csv.DictReader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
    constraintsCount = 0
    currentInstancesCount = 0
    currentViolationsCount = 0
    correctedViolationsAdd = 0
    correctedViolationsDel = 0
    correctedViolationsReplace = 0
    evalWeighted = Eval(math.nan, math.nan, 0)
    evalAverage = Eval(math.nan, math.nan, 0)
    evalDeletionBaselineWeighted = Eval(math.nan, math.nan, 0)
    evalDeletionBaselineAverage = Eval(math.nan, math.nan, 0)
    evalAdditionBaselineWeighted = Eval(math.nan, math.nan, 0)
    evalAdditionBaselineAverage = Eval(math.nan, math.nan, 0)
    for row in reader:
        constraintsCount += 1
        currentInstancesCount += int(row['property instances'])
        currentViolationsCount += int(row['current violations'])
        correctedViolationsAdd += int(row['corrections with one addition'])
        correctedViolationsDel += int(row['corrections with one deletion'])
        correctedViolationsReplace += int(row['corrections with one replacement'])
        evalWeighted = weighted_combination(evalWeighted, read_eval(row, 'mined'))
        evalAverage = average_combination(evalAverage, read_eval(row, 'mined'))
        evalDeletionBaselineWeighted = weighted_combination(evalDeletionBaselineWeighted,
                                                            read_eval(row, 'deletion baseline'))
        evalDeletionBaselineAverage = average_combination(evalDeletionBaselineAverage,
                                                          read_eval(row, 'deletion baseline'))
        evalAdditionBaselineWeighted = weighted_combination(evalAdditionBaselineWeighted,
                                                            read_eval(row, 'addition baseline'))
        evalAdditionBaselineAverage = average_combination(evalAdditionBaselineAverage,
                                                          read_eval(row, 'addition baseline'))

    print("Aggregated stats: " +
          str(constraintsCount) + " constraints, " +
          str(currentInstancesCount) + " current instances, " +
          str(currentViolationsCount) + " current violations, " +
          str(correctedViolationsAdd) + " solved violations with one addition, " +
          str(correctedViolationsDel) + " solved violations with one deletion, " +
          str(correctedViolationsReplace) + " solved violations with one replacement, " +
          str(evalWeighted.precision) + " weighted precision, " +
          str(evalWeighted.recall) + " weighted recall, " +
          str(evalWeighted.f1) + " weighted F-1, " +
          str(evalAverage.precision) + " average precision, " +
          str(evalAverage.recall) + " average recall, " +
          str(evalAverage.f1) + " average F-1," +
          str(evalDeletionBaselineWeighted.precision) + " deletion baseline weighted precision, " +
          str(evalDeletionBaselineWeighted.recall) + " deletion baseline weighted recall, " +
          str(evalDeletionBaselineWeighted.f1) + " deletion baseline weighted F-1, " +
          str(evalDeletionBaselineAverage.precision) + " deletion baseline average precision, " +
          str(evalDeletionBaselineAverage.recall) + " deletion baseline average recall, " +
          str(evalDeletionBaselineAverage.f1) + " deletion baseline average F-1, " +
          str(evalAdditionBaselineWeighted.precision) + " addition baseline weighted precision, " +
          str(evalAdditionBaselineWeighted.recall) + " addition baseline weighted recall, " +
          str(evalAdditionBaselineWeighted.f1) + " addition baseline weighted F-1, " +
          str(evalAdditionBaselineAverage.precision) + " addition baseline average precision, " +
          str(evalAdditionBaselineAverage.recall) + " addition baseline average recall, " +
          str(evalAdditionBaselineAverage.f1) + " addition baseline average F-1.")
