package org.wikidata.history.dataset;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

  public static ConstraintViolationCorrection read(String line, ValueFactory valueFactory, Map<IRI, Constraint> constraints) {
    String[] parts = line.trim().split("\t");
    if (parts.length < 10) {
      throw new IllegalArgumentException("Invalid correction serialization: " + line);
    }
    IRI constraintIRI = NTriplesUtil.parseURI(parts[0], valueFactory);
    if (!constraints.containsKey(constraintIRI)) {
      throw new IllegalArgumentException("Constraint " + constraintIRI + " not found");
    }
    return new ConstraintViolationCorrection(
            constraints.get(constraintIRI),
            valueFactory.createStatement(
                    NTriplesUtil.parseResource(parts[2], valueFactory),
                    NTriplesUtil.parseURI(parts[3], valueFactory),
                    NTriplesUtil.parseValue(parts[4], valueFactory)
            ),
            IntStream.range(0, (parts.length - 6) / 4).mapToObj(i -> valueFactory.createStatement(
                    NTriplesUtil.parseResource(parts[4 * i + 6], valueFactory),
                    NTriplesUtil.parseURI(parts[4 * i + 7], valueFactory),
                    NTriplesUtil.parseValue(parts[4 * i + 8], valueFactory),
                    NTriplesUtil.parseResource(parts[4 * i + 9], valueFactory)
            )).collect(Collectors.toSet()),
            NTriplesUtil.parseURI(parts[1], valueFactory)
    );
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

  public void write(Writer writer) throws IOException {
    writer.append(NTriplesUtil.toNTriplesString(constraint.getId())).append('\t')
            .append(NTriplesUtil.toNTriplesString(correctionRevision)).append('\t')
            .append(NTriplesUtil.toNTriplesString(targetTriple.getSubject())).append('\t')
            .append(NTriplesUtil.toNTriplesString(targetTriple.getPredicate())).append('\t')
            .append(NTriplesUtil.toNTriplesString(targetTriple.getObject())).append("\t->\t")
            .append(correction.stream().map(statement ->
                    NTriplesUtil.toNTriplesString(statement.getSubject()) + '\t' + NTriplesUtil.toNTriplesString(statement.getPredicate()) + '\t' + NTriplesUtil.toNTriplesString(statement.getObject()) + '\t' + NTriplesUtil.toNTriplesString(statement.getContext())
            ).collect(Collectors.joining("\t"))).append('\n');
  }
}
