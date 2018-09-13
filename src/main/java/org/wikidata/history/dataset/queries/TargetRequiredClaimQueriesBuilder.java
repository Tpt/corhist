package org.wikidata.history.dataset.queries;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.wikidata.history.dataset.Constraint;
import org.wikidata.history.sparql.Vocabulary;

import java.util.Arrays;
import java.util.List;

/**
 * https://www.wikidata.org/wiki/Help:Property_constraints_portal/Target_required_claim
 */
public class TargetRequiredClaimQueriesBuilder extends AbstractQueriesBuilder {
  private static final IRI TARGET_CONSTRAINT = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q21510864");

  @Override
  public boolean canBuildForConstraint(Constraint constraint) {
    return constraint.getType().equals(TARGET_CONSTRAINT);
  }

  @Override
  public List<String> buildCorrectionsLookupQueries(Constraint constraint) {
    IRI targetProperty = Vocabulary.toDirectProperty(constraint.getProperty());
    IRI propertyToHave = Vocabulary.toDirectProperty((IRI) constraint.getParameters(PROPERTY_PARAMETER).get(0));
    String valuesToHaveFilter = convertItemParameter(constraint, "o2");

    return Arrays.asList(
            "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) (false AS ?isCorrAddition) (?s AS ?corrS) (?o AS ?corrO) ?corrRev WHERE { " +
                    " GRAPH ?del { ?s <" + targetProperty + "> ?o } . " +
                    " ?corrRev <http://wikiba.se/history/ontology#deletions> ?del ; " +
                    "      <http://wikiba.se/history/ontology#additions> ?add ; " +
                    "      <http://wikiba.se/history/ontology#previousRevision>/<http://wikiba.se/history/ontology#globalState> ?global . " +
                    " FILTER NOT EXISTS { " +
                    valuesToHaveFilter +
                    "    GRAPH ?global { ?o <" + propertyToHave + "> ?o2 } " +
                    " } " +
                    " FILTER NOT EXISTS { GRAPH ?add { ?s <" + OWL.SAMEAS + "> ?red } } " +
                    "}",
            "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) (true AS ?isCorrAddition) (?o AS ?corrS) (<" + propertyToHave + "> AS ?corrP) (?o2 AS ?corrO) ?corrRev WHERE { " +
                    valuesToHaveFilter +
                    " GRAPH ?add { ?o <" + propertyToHave + "> ?o2 } . " +
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
            " FILTER NOT EXISTS { " + valuesToHaveFilter + " ?o <" + propertyToHave + "> ?o2 } " +
            "}";
  }
}
