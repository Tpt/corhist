package org.wikidata.history.corhist.dataset.queries;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.wikidata.history.corhist.dataset.Constraint;
import org.wikidata.history.sparql.Vocabulary;

import java.util.Collections;
import java.util.List;

/**
 * https://www.wikidata.org/wiki/Help:Property_constraints_portal/Unique_value
 */
public class UniqueValueQueriesBuilder extends AbstractQueriesBuilder {
  private static final IRI TARGET_CONSTRAINT = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q21502410");

  @Override
  public boolean canBuildForConstraint(Constraint constraint) {
    return constraint.getType().equals(TARGET_CONSTRAINT);
  }

  @Override
  public List<String> buildCorrectionsLookupQueries(Constraint constraint) {
    IRI directProperty = Vocabulary.toDirectProperty(constraint.getProperty());

    return Collections.singletonList("SELECT DISTINCT ?targetS (?o AS ?targetO) (false AS ?isCorrAddition) (?s AS ?corrS) (?o AS ?corrO) ?corrRev WHERE { " +
            " GRAPH ?del { ?s <" + directProperty + "> ?o } . " +
            " ?corrRev <http://wikiba.se/history/ontology#deletions> ?del ; " +
            "      <http://wikiba.se/history/ontology#additions> ?add ; " +
            "      <http://wikiba.se/history/ontology#previousRevision>/<http://wikiba.se/history/ontology#globalState> ?global . " +
            " GRAPH ?global { ?s2 <" + directProperty + "> ?o } " +
            " FILTER(?s != ?s2) " +
            " FILTER NOT EXISTS { GRAPH ?add { ?s <" + OWL.SAMEAS + "> ?red } } " +
            " { BIND(?s AS ?targetS) } UNION { BIND(?s2 AS ?targetS) } " +
            "}");
  }

  @Override
  public String buildViolationQuery(Constraint constraint, IRI revision) {
    IRI directProperty = Vocabulary.toDirectProperty(constraint.getProperty());

    return "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) FROM <" + Vocabulary.toGlobalState(revision) + "> WHERE { " +
            " ?s <" + directProperty + "> ?o . " +
            " ?s2 <" + directProperty + "> ?o . " +
            " FILTER(?s != ?s2) " +
            "}";
  }
}
