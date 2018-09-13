package org.wikidata.history.dataset.queries;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.wikidata.history.dataset.Constraint;
import org.wikidata.history.sparql.Vocabulary;

import java.util.Arrays;
import java.util.List;

/**
 * https://www.wikidata.org/wiki/Help:Property_constraints_portal/Symmetric
 * https://www.wikidata.org/wiki/Help:Property_constraints_portal/Inverse
 */
public class InverseQueriesBuilder extends AbstractQueriesBuilder {
  private static final IRI INVERSE_CONSTRAINT = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q21510855");
  private static final IRI SYMMETRIC_CONSTRAINT = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q21510862");


  @Override
  public boolean canBuildForConstraint(Constraint constraint) {
    return constraint.getType().equals(INVERSE_CONSTRAINT) || constraint.getType().equals(SYMMETRIC_CONSTRAINT);
  }

  @Override
  public List<String> buildCorrectionsLookupQueries(Constraint constraint) {
    IRI directProperty = Vocabulary.toDirectProperty(constraint.getProperty());
    IRI inverseDirectProperty = constraint.getType().equals(SYMMETRIC_CONSTRAINT)
            ? directProperty
            : constraint.getParameters(PROPERTY_PARAMETER).stream()
            .map(p -> Vocabulary.toDirectProperty((IRI) p))
            .findAny().orElseThrow(() -> new IllegalArgumentException(constraint + " do not have property"));
    return Arrays.asList(
            "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) (false AS ?isCorrAddition) (?s AS ?corrS) (?o AS ?corrO) ?corrRev WHERE { " +
                    " GRAPH ?del { ?s <" + directProperty + "> ?o } . " +
                    " ?corrRev <http://wikiba.se/history/ontology#deletions> ?del ; " +
                    "      <http://wikiba.se/history/ontology#additions> ?add ; " +
                    "      <http://wikiba.se/history/ontology#previousRevision>/<http://wikiba.se/history/ontology#globalState> ?global . " +
                    " FILTER NOT EXISTS { GRAPH ?global { ?o <" + inverseDirectProperty + "> ?s } } " +
                    " FILTER NOT EXISTS { GRAPH ?add { ?s <" + OWL.SAMEAS + "> ?red } } " +
                    "}",
            "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) (true AS ?isCorrAddition) (?o AS ?corrS) (<" + inverseDirectProperty + "> AS ?corrP) (?s AS ?corrO) ?corrRev WHERE { " +
                    " GRAPH ?add { ?o <" + inverseDirectProperty + "> ?s } . " +
                    " ?corrRev <http://wikiba.se/history/ontology#additions> ?add ; " +
                    "      <http://wikiba.se/history/ontology#previousRevision>/<http://wikiba.se/history/ontology#globalState> ?global . " +
                    " GRAPH ?global { ?s <" + directProperty + "> ?o } " +
                    "}"
    );
  }

  @Override
  public String buildViolationQuery(Constraint constraint, IRI revision) {
    IRI directProperty = Vocabulary.toDirectProperty(constraint.getProperty());
    IRI inverseDirectProperty = constraint.getType().equals(SYMMETRIC_CONSTRAINT)
            ? directProperty
            : constraint.getParameters(PROPERTY_PARAMETER).stream()
            .map(p -> Vocabulary.toDirectProperty((IRI) p))
            .findAny().orElseThrow(() -> new IllegalArgumentException(constraint + " do not have property"));
    return "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) FROM <" + Vocabulary.toGlobalState(revision) + "> WHERE { " +
            " ?s <" + directProperty + "> ?o . " +
            " FILTER NOT EXISTS { ?o <" + inverseDirectProperty + "> ?s } . " +
            "}";
  }
}
