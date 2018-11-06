import csv
from typing import List

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


def read_eval(row, prefix, default_precision=math.nan, default_recall=math.nan):
    return Eval(
        row[prefix + ' precision'] if prefix + ' precision' in row else default_precision,
        row[prefix + ' recall'] if prefix + ' recall' in row else default_recall,
        row['test set size']
    )


def weighted_combination(es: List[Eval]):
    filtered_prec = [e for e in es if not math.isnan(e.precision)]
    filtered_rec = [e for e in es if not math.isnan(e.recall)]
    return Eval(
        sum(e.precision * e.count for e in filtered_prec) / sum(e.count for e in filtered_prec)
        if filtered_prec else math.nan,
        sum(e.recall * e.count for e in filtered_rec) / sum(e.count for e in filtered_rec)
        if filtered_rec else math.nan,
        sum(e.count for e in es)
    )


def average_combination(es: List[Eval]):
    filtered_prec = [e for e in es if not math.isnan(e.precision)]
    filtered_rec = [e for e in es if not math.isnan(e.recall)]
    return Eval(
        sum(e.precision for e in filtered_prec) / len(filtered_prec) if filtered_prec else math.nan,
        sum(e.recall for e in filtered_rec) / len(filtered_rec) if filtered_rec else math.nan,
        len(es)
    )


with open(sys.argv[1], newline='') as csvfile:
    reader = csv.DictReader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
    constraintsCount = 0
    currentInstancesCount = 0
    currentViolationsCount = 0
    correctedViolationsAdd = 0
    correctedViolationsDel = 0
    correctedViolationsReplace = 0
    eval = []
    evalDeletionBaseline = []
    evalAdditionBaseline = []
    for row in reader:
        constraintsCount += 1
        currentInstancesCount += int(row['property instances'])
        currentViolationsCount += int(row['current violations'])
        correctedViolationsAdd += int(row['corrections with one addition'])
        correctedViolationsDel += int(row['corrections with one deletion'])
        correctedViolationsReplace += int(row['corrections with one replacement'])
        eval.append(read_eval(row, 'mined'))
        evalDeletionBaseline.append(read_eval(row, 'deletion baseline'))
        evalAdditionBaseline.append(read_eval(row, 'addition baseline'))

    evalWeighted = weighted_combination(eval)
    evalAverage = average_combination(eval)
    evalDeletionBaselineWeighted = weighted_combination(evalDeletionBaseline)
    evalDeletionBaselineAverage = average_combination(evalDeletionBaseline)
    evalAdditionBaselineWeighted = weighted_combination(evalAdditionBaseline)
    evalAdditionBaselineAverage = average_combination(evalAdditionBaseline)

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

    print("Foo & {:.2f} & {:.2f} & {:.2f} & {:.2f} & {:.2f} & {:.2f} \\\\".format(
        evalWeighted.precision, evalWeighted.recall, evalWeighted.f1,
        evalAverage.precision, evalAverage.recall, evalAverage.f1))
    print("Foo & add & {:.2f} & {:.2f} & {:.2f} & {:.2f} & {:.2f} & {:.2f} \\\\".format(
        evalAdditionBaselineWeighted.precision, evalAdditionBaselineWeighted.recall, evalAdditionBaselineWeighted.f1,
        evalAdditionBaselineAverage.precision, evalAdditionBaselineAverage.recall, evalAdditionBaselineAverage.f1))
    print("Foo & deletion & {:.2f} & {:.2f} & {:.2f} & {:.2f} & {:.2f} & {:.2f} \\\\".format(
        evalDeletionBaselineWeighted.precision, evalDeletionBaselineWeighted.recall, evalDeletionBaselineWeighted.f1,
        evalDeletionBaselineAverage.precision, evalDeletionBaselineAverage.recall, evalDeletionBaselineAverage.f1))
