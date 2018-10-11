package org.wikidata.history.mining;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.wikidata.history.dataset.ConstraintViolationCorrection;
import org.wikidata.history.dataset.QueriesForConstraintCorrectionsBuilder;
import org.wikidata.history.sparql.Vocabulary;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * TODO: only works for item and valueType
 */
class AdditionBaseline {

  private final ValueFactory valueFactory;

  AdditionBaseline(ValueFactory valueFactory) {
    this.valueFactory = valueFactory;
  }

  Evaluation compute(Stream<ConstraintViolationCorrection> corrections) {
    int[] allAndFoundAndCorrect = new int[]{0, 0, 0};
    corrections.forEach(correction -> {
      allAndFoundAndCorrect[0]++;
      correction.getConstraint().getParameter(QueriesForConstraintCorrectionsBuilder.PROPERTY_PARAMETER).ifPresent(propertyToHave -> {
        List<Value> values = correction.getConstraint().getParameters(QueriesForConstraintCorrectionsBuilder.ITEM_PARAMETER);
        if (values.size() == 0) {
          allAndFoundAndCorrect[1]++;
          if (correction.getCorrection().equals(Collections.singleton(valueFactory.createStatement(
                  correction.getTargetTriple().getSubject(),
                  Vocabulary.toDirectProperty((IRI) propertyToHave),
                  values.get(0),
                  Vocabulary.HISTORY_ADDITION
          )))) {
            allAndFoundAndCorrect[2]++;
          }
        }
      });
    });
    return new Evaluation(
            ((float) allAndFoundAndCorrect[2]) / allAndFoundAndCorrect[1],
            ((float) allAndFoundAndCorrect[1]) / allAndFoundAndCorrect[0],
            allAndFoundAndCorrect[0]
    );
  }
}
