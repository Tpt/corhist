package org.wikidata.history.corhist.mining;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.wikidata.history.corhist.dataset.ConstraintViolationCorrectionWithContext;
import org.wikidata.history.sparql.Vocabulary;

import java.util.*;

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
    return rules.stream().flatMap(rule ->
            PatternEvaluator.evaluate(rule.getViolationBody(), VALUE_FACTORY.createStatement(
                    correction.getCorrection().getTargetTriple().getSubject(),
                    correction.getCorrection().getConstraint().getId(),
                    correction.getCorrection().getTargetTriple().getObject(),
                    Vocabulary.toGlobalState(Vocabulary.previousRevision(correction.getCorrection().getCorrectionRevision()))
            )).flatMap(violationBindings ->
                    PatternEvaluator.evaluate(rule.getContextBody(), violationBindings, correction.buildContextModel())
                            .map(bindings -> PatternEvaluator.instantiate(rule.getHead(), bindings, VALUE_FACTORY))
            )
    ).findAny();
  }
}
