package org.wikidata.history.corhist.dataset.queries;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.wikidata.history.corhist.dataset.Constraint;
import org.wikidata.history.sparql.Vocabulary;

import java.util.Collections;
import java.util.List;

/**
 * https://www.wikidata.org/wiki/Help:Property_constraints_portal/Single_value
 */
public class SingleValueQueriesBuilder extends AbstractQueriesBuilder {
  private static final IRI TARGET_CONSTRAINT = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q19474404");

  @Override
  public boolean canBuildForConstraint(Constraint constraint) {
    return constraint.getType().equals(TARGET_CONSTRAINT);
  }

  @Override
  public List<String> buildCorrectionsLookupQueries(Constraint constraint) {
    IRI directProperty = Vocabulary.toDirectProperty(constraint.getProperty());
    return Collections.singletonList("SELECT DISTINCT (?s AS ?targetS) ?targetO (false AS ?isCorrAddition) (?s AS ?corrS) (?o AS ?corrO) ?corrRev WHERE { " +
            " GRAPH ?del { ?s <" + directProperty + "> ?o } . " +
            " ?corrRev <http://wikiba.se/history/ontology#deletions> ?del ; " +
            "      <http://wikiba.se/history/ontology#previousRevision>/<http://wikiba.se/history/ontology#globalState> ?global . " +
            " GRAPH ?global { ?s <" + directProperty + "> ?o2 } " +
            " FILTER(?o != ?o2) " +
            " FILTER NOT EXISTS { GRAPH ?del { ?s <" + directProperty + "> ?o2 } } " +
            " { BIND(?o AS ?targetO) } UNION { BIND(?o2 AS ?targetO) } " +
            "}");
  }

  @Override
  public String buildViolationQuery(Constraint constraint, IRI revision) {
    IRI directProperty = Vocabulary.toDirectProperty(constraint.getProperty());

    return "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) FROM <" + Vocabulary.toGlobalState(revision) + "> WHERE { " +
            " ?s <" + directProperty + "> ?o , ?o2 . " +
            " FILTER(?o != ?o2) " +
            "}";
  }
}
