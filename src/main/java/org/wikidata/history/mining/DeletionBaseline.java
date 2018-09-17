package org.wikidata.history.mining;

import org.eclipse.rdf4j.model.ValueFactory;
import org.wikidata.history.dataset.ConstraintViolationCorrection;
import org.wikidata.history.sparql.Vocabulary;

import java.util.Collections;
import java.util.stream.Stream;

class DeletionBaseline {

  private final ValueFactory valueFactory;

  DeletionBaseline(ValueFactory valueFactory) {
    this.valueFactory = valueFactory;
  }

  Evaluation compute(Stream<ConstraintViolationCorrection> corrections) {
    int[] allAndCorrect = new int[]{0, 0};
    corrections.forEach(correction -> {
      allAndCorrect[0]++;
      if (correction.getCorrection().equals(Collections.singleton(valueFactory.createStatement(
              correction.getTargetTriple().getSubject(),
              correction.getTargetTriple().getPredicate(),
              correction.getTargetTriple().getObject(),
              Vocabulary.HISTORY_DELETION
      )))) {
        allAndCorrect[1]++;
      }
    });
    return new Evaluation(((float) allAndCorrect[1]) / allAndCorrect[0], 1, allAndCorrect[0]);
  }
}
