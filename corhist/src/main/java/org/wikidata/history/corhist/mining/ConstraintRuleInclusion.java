package org.wikidata.history.corhist.mining;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

/**
 * Warning: it assumes that two constant vars are equals (i.e. with the same name too)
 */
class ConstraintRuleInclusion {

  /**
   * Returns is a is a more precise rule than b (i.e. if a applies then b applies and returns the same result)
   */
  static boolean isMorePrecise(ConstraintRule a, ConstraintRule b) {
    if (!a.getViolationBody().getPredicateVar().equals(b.getViolationBody().getPredicateVar())) {
      return false;
    }

    QueryBindingSet additionalBindingsForB = new QueryBindingSet();

    //Violation body
    return isStatementPatternMorePrecise(a.getViolationBody(), b.getViolationBody(), additionalBindingsForB) &&
            a.getContextBody().stream().allMatch(aContextStatement ->
                    b.getContextBody().stream().anyMatch(bContextStatement ->
                            isStatementPatternMorePrecise(aContextStatement, bContextStatement, additionalBindingsForB)
                    )
            ) &&
            a.getHead().equals(b.getHead());
  }

  private static boolean isStatementPatternMorePrecise(StatementPattern a, StatementPattern b, QueryBindingSet additionalBindingsForB) {
    return isVarSelectionMorePrecise(a.getSubjectVar(), b.getSubjectVar(), additionalBindingsForB) &&
            isVarSelectionMorePrecise(a.getPredicateVar(), b.getPredicateVar(), additionalBindingsForB) &&
            isVarSelectionMorePrecise(a.getObjectVar(), b.getObjectVar(), additionalBindingsForB) &&
            isVarSelectionMorePrecise(a.getContextVar(), b.getContextVar(), additionalBindingsForB);
  }

  private static boolean isVarSelectionMorePrecise(Var a, Var b, QueryBindingSet additionalBindingsForB) {
    if (a == null || b == null) {
      return a == b;
    } else if (a.isConstant()) {
      if (b.isConstant()) {
        return a.equals(b);
      } else {
        if (additionalBindingsForB.hasBinding(b.getName())) {
          return a.getValue().equals(additionalBindingsForB.getBinding(b.getName()));
        } else {
          additionalBindingsForB.addBinding(b.getName(), a.getValue());
          return true;
        }
      }
    } else {
      return a.equals(b);
    }
  }
}
