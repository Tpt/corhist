package org.wikidata.history.corhist.mining;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.wikidata.history.corhist.dataset.ConstraintViolationCorrectionWithContext;
import org.wikidata.history.corhist.mining.ConstraintRuleWithContext.ContextualBinding;
import org.wikidata.history.sparql.Vocabulary;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class EvaluatorWithContext {

  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  Evaluation evaluate(List<ConstraintRuleWithContext> rules, List<ConstraintViolationCorrectionWithContext> corrections) {
    if (corrections.isEmpty()) {
      throw new IllegalArgumentException("The test set should not be empty");
    }

    rules.sort(Comparator.reverseOrder());

    int goodCorrections = 0;
    int badCorrections = 0;
    int someCorrectionFound = 0;
    int noCorrectionFound = 0;
    for (ConstraintViolationCorrectionWithContext correction : corrections) {
      Set<Statement> possibleCorrection = buildPossibleCorrection(rules, correction).orElseGet(Collections::emptySet);
      if (possibleCorrection.isEmpty()) {
        noCorrectionFound++;
      } else {
        someCorrectionFound++;
        if (possibleCorrection.equals(correction.getCorrection().getCorrection())) {
          goodCorrections++;
        } else {
          //System.out.println("Expected:" + correction.getCorrection());
          //System.out.println("Actual:" + possibleCorrection);
          badCorrections++;
        }
      }
    }

    assert goodCorrections + badCorrections == someCorrectionFound;
    assert someCorrectionFound + noCorrectionFound == corrections.size();

    return new Evaluation(
            (someCorrectionFound == 0) ? 1 : ((float) goodCorrections) / someCorrectionFound,
            ((float) goodCorrections) / corrections.size(),
            corrections.size()
    );
  }

  private Optional<Set<Statement>> buildPossibleCorrection(List<ConstraintRuleWithContext> rules, ConstraintViolationCorrectionWithContext correction) {
    Statement violationStatement = VALUE_FACTORY.createStatement(
            correction.getCorrection().getTargetTriple().getSubject(),
            correction.getCorrection().getConstraint().getId(),
            correction.getCorrection().getTargetTriple().getObject(),
            Vocabulary.toGlobalState(Vocabulary.previousRevision(correction.getCorrection().getCorrectionRevision()))
    );
    ContextualBinding base = new ContextualBinding(correction);
    return rules.stream().flatMap(rule ->
            evaluate(rule.getViolationBody(), violationStatement, base)
                    .flatMap(binding -> {
                      Stream<ContextualBinding> bindings = Stream.of(binding);
                      for (ConstraintRuleWithContext.ContextPattern p : rule.getContextPatterns()) {
                        bindings = bindings.flatMap(b -> b.evaluate(p));
                      }
                      return bindings;
                    })
                    .map(bindings -> instantiate(rule.getHead(), bindings))
    ).findAny();
  }

  private Stream<ContextualBinding> evaluate(StatementPattern pattern, Statement statement, ContextualBinding base) {
    Var object = pattern.getObjectVar();
    if (object.isConstant()) {
      if (!object.getValue().equals(statement.getObject())) {
        return Stream.empty();
      }
    } else {
      base = base.withValue(object.getName(), statement.getObject());
    }

    Var predicate = pattern.getPredicateVar();
    if (predicate.isConstant()) {
      if (!predicate.getValue().equals(statement.getPredicate())) {
        return Stream.empty();
      }
    } else {
      base = base.withValue(predicate.getName(), statement.getPredicate());
    }

    Var subject = pattern.getSubjectVar();
    if (subject.isConstant()) {
      if (!subject.getValue().equals(statement.getSubject())) {
        return Stream.empty();
      }
    } else {
      base = base.withValue(subject.getName(), statement.getSubject());
    }

    return Stream.of(base);
  }

  private Set<Statement> instantiate(Collection<StatementPattern> patterns, ContextualBinding binding) {
    return patterns.stream().map(pattern -> instantiate(pattern, binding)).collect(Collectors.toSet());
  }

  private Statement instantiate(StatementPattern pattern, ContextualBinding binding) {
    return VALUE_FACTORY.createStatement(
            (Resource) (pattern.getSubjectVar().isConstant() ? pattern.getSubjectVar().getValue() : binding.getValue(pattern.getSubjectVar().getName())),
            (IRI) pattern.getPredicateVar().getValue(),
            pattern.getObjectVar().isConstant() ? pattern.getObjectVar().getValue() : binding.getValue(pattern.getObjectVar().getName()),
            (Resource) pattern.getContextVar().getValue()
    );
  }
}
