package org.wikidata.history.corhist.dataset.queries;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.wikidata.history.corhist.dataset.Constraint;
import org.wikidata.history.sparql.Vocabulary;

import java.util.Arrays;
import java.util.List;

/**
 * https://www.wikidata.org/wiki/Help:Property_constraints_portal/Conflicts_with
 */
public class ConflictsWithQueriesBuilder extends AbstractQueriesBuilder {
  private static final IRI TARGET_CONSTRAINT = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q21502838");

  @Override
  public boolean canBuildForConstraint(Constraint constraint) {
    return constraint.getType().equals(TARGET_CONSTRAINT);
  }

  @Override
  public List<String> buildCorrectionsLookupQueries(Constraint constraint) {
    IRI targetProperty = Vocabulary.toDirectProperty(constraint.getProperty());
    IRI propertyInConflict = Vocabulary.toDirectProperty((IRI) constraint.getParameters(PROPERTY_PARAMETER).get(0));
    String valuesInConflictFilter = convertItemParameter(constraint, "o2");

    return Arrays.asList(
            "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) (false AS ?isCorrAddition) (?s AS ?corrS) (?o AS ?corrO) ?corrRev WHERE { " +
                    " GRAPH ?del { ?s <" + targetProperty + "> ?o } . " +
                    " ?corrRev <http://wikiba.se/history/ontology#deletions> ?del ; " +
                    "      <http://wikiba.se/history/ontology#previousRevision>/<http://wikiba.se/history/ontology#globalState> ?global . " +
                    " GRAPH ?global { " + valuesInConflictFilter + " ?s <" + propertyInConflict + "> ?o2 } " +
                    " FILTER NOT EXISTS { GRAPH ?del { ?s <" + propertyInConflict + "> ?o2 } } " +
                    "}",
            "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) (false AS ?isCorrAddition) (?s AS ?corrS) (<" + propertyInConflict + "> AS ?corrP) (?o2 AS ?corrO) ?corrRev WHERE { " +
                    valuesInConflictFilter +
                    " GRAPH ?del { ?s <" + propertyInConflict + "> ?o2 } . " +
                    " ?corrRev <http://wikiba.se/history/ontology#deletions> ?del ; " +
                    "      <http://wikiba.se/history/ontology#previousRevision>/<http://wikiba.se/history/ontology#globalState> ?global . " +
                    " GRAPH ?global { ?s <" + targetProperty + "> ?o } " +
                    " FILTER NOT EXISTS { GRAPH ?del { ?s <" + targetProperty + "> ?o } } " +
                    "}"
    );
  }

  @Override
  public String buildViolationQuery(Constraint constraint, IRI revision) {
    IRI targetProperty = Vocabulary.toDirectProperty(constraint.getProperty());
    IRI propertyInConflict = Vocabulary.toDirectProperty((IRI) constraint.getParameters(PROPERTY_PARAMETER).get(0));
    String valuesInConflictFilter = convertItemParameter(constraint, "o2");

    return "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) FROM <" + Vocabulary.toGlobalState(revision) + "> WHERE { " +
            " ?s <" + targetProperty + "> ?o . " +
            valuesInConflictFilter + " ?s <" + propertyInConflict + "> ?o2 ." +
            "}";
  }
}
