package org.wikidata.history.corhist.mining;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.wikidata.history.corhist.ConvertingValueFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public final class SimpleConstraintRule implements Serializable {

  private static final ConvertingValueFactory VALUE_FACTORY = new ConvertingValueFactory(SimpleValueFactory.getInstance());

  private List<StatementPattern> head;
  private StatementPattern violationBody;
  private List<StatementPattern> contextBody;

  public SimpleConstraintRule(Set<StatementPattern> head, StatementPattern violationBody, List<StatementPattern> contextBody) {
    this.head = head.stream().map(SimpleConstraintRule::convert).collect(Collectors.toList());
    this.violationBody = convert(violationBody);
    this.contextBody = contextBody.stream().map(SimpleConstraintRule::convert).collect(Collectors.toList());
  }

  private static StatementPattern convert(StatementPattern statementPattern) {
    return new StatementPattern(
            statementPattern.getScope(),
            convert(statementPattern.getSubjectVar()),
            convert(statementPattern.getPredicateVar()),
            convert(statementPattern.getObjectVar()),
            convert(statementPattern.getContextVar())
    );
  }

  private static Var convert(Var var) {
    if (var != null && var.getValue() != null) {
      var.setValue(VALUE_FACTORY.convert(var.getValue()));
    }
    return var;
  }

  public List<StatementPattern> getHead() {
    return head;
  }

  public StatementPattern getViolationBody() {
    return violationBody;
  }

  public List<StatementPattern> getContextBody() {
    return contextBody;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SimpleConstraintRule)) {
      return false;
    }
    SimpleConstraintRule o = (SimpleConstraintRule) other;
    return head.equals(o.head) && violationBody.equals(o.violationBody) && contextBody.equals(o.contextBody);
  }

  @Override
  public int hashCode() {
    return head.hashCode() ^ violationBody.hashCode();
  }
}
