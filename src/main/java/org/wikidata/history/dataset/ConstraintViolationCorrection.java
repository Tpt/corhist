package org.wikidata.history.dataset;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;

import java.util.Set;

public final class ConstraintViolationCorrection {
  private Constraint constraint;
  private Statement targetTriple;
  private Set<Statement> correction;
  private IRI correctionRevision;

  ConstraintViolationCorrection(Constraint constraint, Statement targetTriple, Set<Statement> correction, IRI correctionRevision) {
    this.constraint = constraint;
    this.targetTriple = targetTriple;
    this.correction = correction;
    this.correctionRevision = correctionRevision;
  }

  public Constraint getConstraint() {
    return constraint;
  }

  public Statement getTargetTriple() {
    return targetTriple;
  }

  public Set<Statement> getCorrection() {
    return correction;
  }

  public IRI getCorrectionRevision() {
    return correctionRevision;
  }

  @Override
  public String toString() {
    return correction.toString() + " at revision " + correctionRevision +
            " to fix the error on " + targetTriple +
            " for constraint " + constraint;
  }
}
