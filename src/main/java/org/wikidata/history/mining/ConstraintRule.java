package org.wikidata.history.mining;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public final class ConstraintRule implements Comparable<ConstraintRule> {

  private Set<StatementPattern> head;
  private StatementPattern violationBody;
  private List<StatementPattern> contextBody;
  private List<BindingSet> bodyBindings;
  private List<BindingSet> fullBindings;

  ConstraintRule(Set<StatementPattern> head, StatementPattern violationBody, List<StatementPattern> contextBody, List<BindingSet> bodyBindings, List<BindingSet> fullBindings) {
    this.head = head;
    this.violationBody = violationBody;
    this.contextBody = contextBody;
    this.bodyBindings = bodyBindings;
    this.fullBindings = fullBindings;
  }

  ConstraintRule(Set<StatementPattern> head, StatementPattern violationBody, List<BindingSet> bodyBindings, List<BindingSet> fullBindings) {
    this(head, violationBody, Collections.emptyList(), bodyBindings, fullBindings);
  }

  public Set<StatementPattern> getHead() {
    return head;
  }

  public StatementPattern getViolationBody() {
    return violationBody;
  }

  public List<StatementPattern> getContextBody() {
    return contextBody;
  }

  Stream<BindingSet> getBodyBindings() {
    return bodyBindings.stream();
  }

  Stream<BindingSet> getFullBindings() {
    return fullBindings.stream();
  }

  public int getSupport() {
    return fullBindings.size();
  }

  public float getStdConfidence() {
    return fullBindings.size() / (float) bodyBindings.size();
  }

  public SimpleConstraintRule toSimple() {
    return new SimpleConstraintRule(head, violationBody, contextBody);
  }

  @Override
  public int compareTo(ConstraintRule r) {
    int comp = Float.compare(getStdConfidence(), r.getStdConfidence());
    return (comp == 0) ? Integer.compare(fullBindings.size(), r.fullBindings.size()) : comp;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ConstraintRule)) {
      return false;
    }
    ConstraintRule o = (ConstraintRule) other;
    return head.equals(o.head) && violationBody.equals(o.violationBody) && contextBody.equals(o.contextBody);
  }

  @Override
  public int hashCode() {
    return head.hashCode() ^ violationBody.hashCode();
  }
}
