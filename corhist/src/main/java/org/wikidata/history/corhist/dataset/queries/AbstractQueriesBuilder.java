package org.wikidata.history.corhist.dataset.queries;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.corhist.dataset.Constraint;
import org.wikidata.history.corhist.dataset.QueriesForConstraintCorrectionsBuilder;
import org.wikidata.history.sparql.Vocabulary;

import java.util.List;
import java.util.stream.Collectors;

abstract class AbstractQueriesBuilder implements QueriesForConstraintCorrectionsBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractQueriesBuilder.class);

  String convertRelationParameter(Constraint constraint) {
    Value relation = constraint.getParameter(RELATION_PARAMETER)
            .orElseGet(() -> {
              LOGGER.warn("Found no value for relation parameter, assuming instance of, for constraint " + constraint.getId());
              return INSTANCEOF_ENTITY;
            });
    if (relation.equals(INSTANCEOF_ENTITY)) {
      return "<" + Vocabulary.toDirectProperty(INSTANCEOF_PROPERTY) + ">";
    } else if (relation.equals(SUBCLASSOF_ENTITY)) {
      return "<" + Vocabulary.toDirectProperty(SUBCLASSOF_PROPERTY) + ">";
    } else if (relation.equals(INSTANCE_OR_SUBCLASS_OF_PROPERTY)) {
      return "(<" + Vocabulary.toDirectProperty(INSTANCEOF_PROPERTY) + ">|<" + Vocabulary.toDirectProperty(SUBCLASSOF_PROPERTY) + ">)";
    } else {
      throw new IllegalArgumentException("Not supported relation: " + relation + " in constraint " + constraint.getId());
    }
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

  protected String buildSamplingConstraint(String variableName, long instancesCount) {
    long div = Math.max(1L, instancesCount / 1_000_000L);

    return " ?" + variableName + " <http://wikiba.se/history/ontology#revisionId> ?revId FILTER(?revId / " + div + " = ROUND(?revId / " + div + ")) ";
  }
}
