package org.wikidata.history.corhist.mining;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.wikidata.history.corhist.dataset.ConstraintViolationCorrection;
import org.wikidata.history.sparql.Vocabulary;

import java.util.*;

class Evaluator {

  private final ValueFactory valueFactory;
  private final RepositoryConnection repositoryConnection;

  Evaluator(RepositoryConnection repositoryConnection) {
    valueFactory = repositoryConnection.getValueFactory();
    this.repositoryConnection = repositoryConnection;
  }

  Evaluation evaluate(List<ConstraintRule> rules, List<ConstraintViolationCorrection> corrections) {
    if (corrections.isEmpty()) {
      throw new IllegalArgumentException("The test set should not be empty");
    }

    rules.sort(Comparator.reverseOrder());

    int goodCorrections = 0;
    int badCorrections = 0;
    int someCorrectionFound = 0;
    int noCorrectionFound = 0;
    for (ConstraintViolationCorrection correction : corrections) {
      Set<Statement> possibleCorrection = buildPossibleCorrection(rules, correction).orElseGet(Collections::emptySet);
      if (possibleCorrection.isEmpty()) {
        noCorrectionFound++;
      } else {
        someCorrectionFound++;
        if (possibleCorrection.equals(correction.getCorrection())) {
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
            ((float) someCorrectionFound) / corrections.size(),
            corrections.size()
    );
  }

  private Optional<Set<Statement>> buildPossibleCorrection(List<ConstraintRule> rules, ConstraintViolationCorrection correction) {
    return rules.stream().flatMap(rule ->
            PatternEvaluator.evaluate(rule.getViolationBody(), valueFactory.createStatement(
                    correction.getTargetTriple().getSubject(),
                    correction.getConstraint().getId(),
                    correction.getTargetTriple().getObject(),
                    Vocabulary.toGlobalState(Vocabulary.previousRevision(correction.getCorrectionRevision()))
            )).flatMap(violationBindings ->
                    PatternEvaluator.evaluate(rule.getContextBody(), violationBindings, repositoryConnection)
                            .map(bindings -> PatternEvaluator.instantiate(rule.getHead(), bindings, valueFactory))
            )
    ).findAny();
  }
}
