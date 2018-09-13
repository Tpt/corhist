package org.wikidata.history.mining;

import org.wikidata.history.dataset.ConstraintViolationCorrection;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class TuningMiner {

  private static final int SPLIT_THRESHOLD = 100;
  private static final double CROSS_VALIDATION_SET_RATIO = 0.10;

  private final Miner miner;
  private final Evaluator evaluator;

  TuningMiner(Miner miner, Evaluator evaluator) {
    this.miner = miner;
    this.evaluator = evaluator;
  }

  List<ConstraintRule> mine(List<ConstraintViolationCorrection> corrections) {
    //We separate a test set only if we have enough data
    List<ConstraintViolationCorrection> trainSet, crossValidationSet;
    if (corrections.size() > SPLIT_THRESHOLD) {
      trainSet = new ArrayList<>(corrections.size());
      crossValidationSet = new ArrayList<>(corrections.size());
      corrections.forEach(correction -> {
        if (Math.random() >= CROSS_VALIDATION_SET_RATIO) {
          crossValidationSet.add(correction);
        } else {
          trainSet.add(correction);
        }
      });
    } else {
      trainSet = corrections;
      crossValidationSet = corrections;
    }

    // We do the cross validation
    List<ConstraintRule> bestRules = miner.mine(trainSet);
    float bestF1 = evaluator.evaluate(bestRules, crossValidationSet).getF1();

    List<ConstraintRule> currentRules = bestRules;
    for (float newStdConfidence = Miner.MIN_STD_CONFIDENCE + 0.05f; newStdConfidence <= 1; newStdConfidence += 0.05f) {
      float filterStdConfidence = newStdConfidence;
      currentRules = currentRules.stream().filter(rule -> rule.getStdConfidence() >= filterStdConfidence).collect(Collectors.toList());
      float newF1 = evaluator.evaluate(currentRules, crossValidationSet).getF1();
      if (newF1 > bestF1) {
        bestRules = currentRules;
        bestF1 = newF1;
      }
    }

    return bestRules;
  }
}
