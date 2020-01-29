package org.wikidata.history.corhist.mining;

import org.wikidata.history.corhist.dataset.ConstraintViolationCorrectionWithContext;

import java.util.List;
import java.util.stream.Collectors;

class TuningMinerFromContext {

  private final MinerFromContext miner;
  private final EvaluatorWithContext evaluator;

  TuningMinerFromContext(MinerFromContext miner, EvaluatorWithContext evaluator) {
    this.miner = miner;
    this.evaluator = evaluator;
  }

  List<ConstraintRuleWithContext> mine(List<ConstraintViolationCorrectionWithContext> trainCorrections, List<ConstraintViolationCorrectionWithContext> crossValidationCorrections) {
    if (crossValidationCorrections.isEmpty()) {
      crossValidationCorrections = trainCorrections; //TODO: bad hack
    }

    // We do the cross validation
    List<ConstraintRuleWithContext> bestRules = miner.mine(trainCorrections);
    float bestF1 = evaluator.evaluate(bestRules, crossValidationCorrections).getF1();

    List<ConstraintRuleWithContext> currentRules = bestRules;
    for (float newStdConfidence = Miner.MIN_STD_CONFIDENCE + 0.05f; newStdConfidence <= 1; newStdConfidence += 0.05f) {
      float filterStdConfidence = newStdConfidence;
      currentRules = currentRules.stream().filter(rule -> rule.getStdConfidence() >= filterStdConfidence).collect(Collectors.toList());
      float newF1 = evaluator.evaluate(currentRules, crossValidationCorrections).getF1();
      if (newF1 > bestF1) {
        bestRules = currentRules;
        bestF1 = newF1;
      }
    }

    return bestRules;
  }
}
