package org.wikidata.history.dataset.queries;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.dataset.Constraint;
import org.wikidata.history.dataset.QueriesForConstraintCorrectionsBuilder;
import org.wikidata.history.sparql.Vocabulary;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class AbstractQueriesBuilder implements QueriesForConstraintCorrectionsBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractQueriesBuilder.class);

  String convertRelationParameter(Constraint constraint) {
    return "(" + constraint.getParameters(RELATION_PARAMETER).stream().flatMap(relation -> {
      if (relation.equals(INSTANCEOF_ENTITY)) {
        return Stream.of(INSTANCEOF_PROPERTY);
      } else if (relation.equals(SUBCLASSOF_ENTITY)) {
        return Stream.of(SUBCLASSOF_PROPERTY);
      } else if (relation.equals(INSTANCE_OR_SUBCLASS_OF_PROPERTY)) {
        return Stream.of(INSTANCEOF_PROPERTY, SUBCLASSOF_PROPERTY);
      } else {
        LOGGER.error("Not supported relation: " + relation);
        return Stream.of(NOT_EXISTING_PROPERTY);
      }
    }).distinct().map(p -> "<" + Vocabulary.toDirectProperty(p) + ">").collect(Collectors.joining("|")) + ")";
  }

  String convertClassParameter(Constraint constraint, String variableName) {
    return convertValues(constraint.getParameters(CLASS_PARAMETER), variableName);
  }

  String convertItemParameter(Constraint constraint, String variableName) {
    return convertValues(constraint.getParameters(ITEM_PARAMETER), variableName);
  }

  private String convertValues(List<Value> values, String variableName) {
    if (values.isEmpty()) {
      return "";
    }
    if (values.size() == 1 && values.get(0) instanceof IRI) {
      return " BIND(<" + values.get(0).toString() + "> AS ?" + variableName + ") ";
    }
    return " VALUES ?" + variableName + " { " + values.stream()
            .filter(item -> item instanceof IRI) //TODO: some and no value
            .map(item -> "<" + item.toString() + ">")
            .collect(Collectors.joining(" ")) + "} ";
  }
}
