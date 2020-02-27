package org.wikidata.history.corhist.mining;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.wikidata.history.corhist.dataset.ConstraintViolationCorrectionWithContext;
import org.wikidata.history.corhist.mining.ConstraintRuleWithContext.ContextualBinding;

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
    ContextualBinding base = new ContextualBinding(correction);
    return rules.stream().flatMap(rule ->
            base.evaluate(rule.getViolationBody())
                    .flatMap(binding -> {
                      Stream<ContextualBinding> bindings = Stream.of(binding);
                      for (ConstraintRuleWithContext.SimplePattern p : rule.getContextBody()) {
                        bindings = bindings.flatMap(b -> b.evaluate(p));
                      }
                      return bindings;
                    })
                    .map(bindings -> instantiate(rule.getHead(), bindings))
    ).findAny();
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
