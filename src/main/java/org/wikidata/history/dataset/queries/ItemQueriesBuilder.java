package org.wikidata.history.dataset.queries;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.wikidata.history.dataset.Constraint;
import org.wikidata.history.sparql.Vocabulary;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * https://www.wikidata.org/wiki/Help:Property_constraints_portal/Item
 */
public class ItemQueriesBuilder extends AbstractQueriesBuilder {
  private static final IRI TARGET_CONSTRAINT = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q21503247");

  private final boolean onlyWithValues;

  public ItemQueriesBuilder() {
    this(false);
  }

  public ItemQueriesBuilder(boolean onlyWithValues) {
    this.onlyWithValues = onlyWithValues;
  }

  @Override
  public boolean canBuildForConstraint(Constraint constraint) {
    return constraint.getType().equals(TARGET_CONSTRAINT);
  }

  @Override
  public List<String> buildCorrectionsLookupQueries(Constraint constraint) {
    IRI targetProperty = Vocabulary.toDirectProperty(constraint.getProperty());
    IRI propertyToHave = Vocabulary.toDirectProperty((IRI) constraint.getParameters(PROPERTY_PARAMETER).get(0));
    String valuesToHaveFilter = convertItemParameter(constraint, "o2");
    if (onlyWithValues && valuesToHaveFilter.isEmpty()) {
      return Collections.emptyList();
    }

    return Arrays.asList(
            "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) (false AS ?isCorrAddition) (?s AS ?corrS) (?o AS ?corrO) ?corrRev WHERE { " +
                    " GRAPH ?del { ?s <" + targetProperty + "> ?o } . " +
                    " ?corrRev <http://wikiba.se/history/ontology#deletions> ?del ; " +
                    "      <http://wikiba.se/history/ontology#additions> ?add ; " +
                    "      <http://wikiba.se/history/ontology#previousRevision>/<http://wikiba.se/history/ontology#globalState> ?global . " +
                    " FILTER NOT EXISTS { GRAPH ?global { " + valuesToHaveFilter + " ?s <" + propertyToHave + "> ?o2 } } " +
                    " FILTER NOT EXISTS { GRAPH ?add { ?s <" + OWL.SAMEAS + "> ?red } } " +
                    "}",
            "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) (true AS ?isCorrAddition) (?s AS ?corrS) (<" + propertyToHave + "> AS ?corrP) (?o2 AS ?corrO) ?corrRev WHERE { " +
                    valuesToHaveFilter +
                    " GRAPH ?add { ?s <" + propertyToHave + "> ?o2 } . " +
                    " ?corrRev <http://wikiba.se/history/ontology#additions> ?add ; " +
                    "      <http://wikiba.se/history/ontology#previousRevision>/<http://wikiba.se/history/ontology#globalState> ?global . " +
                    " GRAPH ?global { ?s <" + targetProperty + "> ?o } " +
                    "}"
    );
  }

  @Override
  public String buildViolationQuery(Constraint constraint, IRI revision) {
    IRI targetProperty = Vocabulary.toDirectProperty(constraint.getProperty());
    IRI propertyToHave = Vocabulary.toDirectProperty((IRI) constraint.getParameters(PROPERTY_PARAMETER).get(0));
    String valuesToHaveFilter = convertItemParameter(constraint, "o2");

    return "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) FROM <" + Vocabulary.toGlobalState(revision) + "> WHERE { " +
            " ?s <" + targetProperty + "> ?o . " +
            " FILTER NOT EXISTS { " + valuesToHaveFilter + " ?s <" + propertyToHave + "> ?o2 } " +
            "}";
  }
}
