package org.wikidata.history.corhist.mining;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public final class ConstraintRuleWithContext implements Comparable<ConstraintRuleWithContext> {

  private Set<StatementPattern> head;
  private StatementPattern violationBody;
  private List<StatementPattern> contextBody;
  private List<Map.Entry<BindingSet, Model>> bodyBindings;
  private List<Map.Entry<BindingSet, Model>> fullBindings;

  ConstraintRuleWithContext(Set<StatementPattern> head, StatementPattern violationBody, List<StatementPattern> contextBody, List<Map.Entry<BindingSet, Model>> bodyBindings, List<Map.Entry<BindingSet, Model>> fullBindings) {
    this.head = head;
    this.violationBody = violationBody;
    this.contextBody = contextBody;
    this.bodyBindings = bodyBindings;
    this.fullBindings = fullBindings;
  }

  ConstraintRuleWithContext(Set<StatementPattern> head, StatementPattern violationBody, List<Map.Entry<BindingSet, Model>> bodyBindings, List<Map.Entry<BindingSet, Model>> fullBindings) {
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

  Stream<Map.Entry<BindingSet, Model>> getBodyBindings() {
    return bodyBindings.stream();
  }

  Stream<Map.Entry<BindingSet, Model>> getFullBindings() {
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
  public int compareTo(ConstraintRuleWithContext r) {
    int comp = Float.compare(getStdConfidence(), r.getStdConfidence());
    return (comp == 0) ? Integer.compare(fullBindings.size(), r.fullBindings.size()) : comp;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ConstraintRuleWithContext)) {
      return false;
    }
    ConstraintRuleWithContext o = (ConstraintRuleWithContext) other;
    return head.equals(o.head) && violationBody.equals(o.violationBody) && contextBody.equals(o.contextBody);
  }

  @Override
  public int hashCode() {
    return head.hashCode() ^ violationBody.hashCode();
  }
}
