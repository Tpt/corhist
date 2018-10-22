package org.wikidata.history.mining;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.wikidata.history.dataset.Constraint;
import org.wikidata.history.dataset.ConstraintViolationCorrection;
import org.wikidata.history.sparql.Vocabulary;

import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import static org.wikidata.history.dataset.QueriesForConstraintCorrectionsBuilder.*;

class AdditionBaseline {

  private final ValueFactory valueFactory;

  AdditionBaseline(ValueFactory valueFactory) {
    this.valueFactory = valueFactory;
  }

  Evaluation compute(Stream<ConstraintViolationCorrection> corrections) {
    int[] allAndFoundAndCorrect = new int[]{0, 0, 0};
    corrections.forEach(correction -> {
      allAndFoundAndCorrect[0]++;
      buildStatement(correction).ifPresent(possibleCorrection -> {
        allAndFoundAndCorrect[1]++;
        if (correction.getCorrection().equals(Collections.singleton(possibleCorrection))) {
          allAndFoundAndCorrect[2]++;
        }
      });
    });
    return new Evaluation(
            ((float) allAndFoundAndCorrect[2]) / allAndFoundAndCorrect[1],
            ((float) allAndFoundAndCorrect[1]) / allAndFoundAndCorrect[0],
            allAndFoundAndCorrect[0]
    );
  }

  private Optional<Statement> buildStatement(ConstraintViolationCorrection correction) {
    Constraint constraint = correction.getConstraint();
    switch (constraint.getType().getLocalName()) {
      case "Q21510855":
        //Inverse
        return correction.getConstraint().getParameter(PROPERTY_PARAMETER).map(propertyToHave ->
                valueFactory.createStatement(
                        (Resource) correction.getTargetTriple().getObject(),
                        Vocabulary.toDirectProperty((IRI) propertyToHave),
                        correction.getTargetTriple().getSubject(),
                        Vocabulary.HISTORY_ADDITION
                )
        );
      case "Q21510862":
        //Symmetric
        return Optional.of(valueFactory.createStatement(
                (Resource) correction.getTargetTriple().getObject(),
                correction.getTargetTriple().getPredicate(),
                correction.getTargetTriple().getSubject(),
                Vocabulary.HISTORY_ADDITION
        ));
      case "Q21503247":
        //Item
        return correction.getConstraint().getParameter(PROPERTY_PARAMETER).flatMap(propertyToHave ->
                correction.getConstraint().getParameter(ITEM_PARAMETER).map(value ->
                        valueFactory.createStatement(
                                correction.getTargetTriple().getSubject(),
                                Vocabulary.toDirectProperty((IRI) propertyToHave),
                                value,
                                Vocabulary.HISTORY_ADDITION
                        )
                )
        );
      case "Q21510864":
        //Target require claim
        return correction.getConstraint().getParameter(PROPERTY_PARAMETER).flatMap(propertyToHave ->
                correction.getConstraint().getParameter(ITEM_PARAMETER).map(value ->
                        valueFactory.createStatement(
                                (Resource) correction.getTargetTriple().getObject(),
                                Vocabulary.toDirectProperty((IRI) propertyToHave),
                                value,
                                Vocabulary.HISTORY_ADDITION
                        )
                )
        );
      case "Q21503250":
        //Type
        return getRelationParameter(constraint).flatMap(propertyToHave -> constraint.getParameter(CLASS_PARAMETER).map(type ->
                valueFactory.createStatement(
                        correction.getTargetTriple().getSubject(),
                        Vocabulary.toDirectProperty(propertyToHave),
                        type,
                        Vocabulary.HISTORY_ADDITION
                )
        ));
      case "Q21510865":
        //Value Type
        return getRelationParameter(constraint).flatMap(propertyToHave -> constraint.getParameter(CLASS_PARAMETER).map(type ->
                valueFactory.createStatement(
                        (Resource) correction.getTargetTriple().getObject(),
                        Vocabulary.toDirectProperty(propertyToHave),
                        type,
                        Vocabulary.HISTORY_ADDITION
                )
        ));
      default:
        return Optional.empty();
    }
  }

  private Optional<IRI> getRelationParameter(Constraint constraint) {
    return constraint.getParameter(RELATION_PARAMETER).flatMap(relation -> {
      if (relation.equals(INSTANCEOF_ENTITY)) {
        return Optional.of(INSTANCEOF_PROPERTY);
      } else if (relation.equals(SUBCLASSOF_ENTITY)) {
        return Optional.of(SUBCLASSOF_PROPERTY);
      } else {
        return Optional.empty();
      }
    });
  }
}
