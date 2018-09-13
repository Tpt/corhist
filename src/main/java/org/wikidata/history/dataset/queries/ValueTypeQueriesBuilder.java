package org.wikidata.history.dataset.queries;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.wikidata.history.dataset.Constraint;
import org.wikidata.history.sparql.Vocabulary;

import java.util.Arrays;
import java.util.List;

/**
 * https://www.wikidata.org/wiki/Help:Property_constraints_portal/Value_type
 */
public class ValueTypeQueriesBuilder extends AbstractQueriesBuilder {
  private static final IRI TARGET_CONSTRAINT = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q21510865");

  @Override
  public boolean canBuildForConstraint(Constraint constraint) {
    return constraint.getType().equals(TARGET_CONSTRAINT);
  }

  @Override
  public List<String> buildCorrectionsLookupQueries(Constraint constraint) {
    IRI targetProperty = Vocabulary.toDirectProperty(constraint.getProperty());
    String relationsToHave = convertRelationParameter(constraint);
    String typesToHaveFilter = convertClassParameter(constraint, "o2");

    return Arrays.asList(
            "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) (false AS ?isCorrAddition) (?s AS ?corrS) (?o AS ?corrO) ?corrRev WHERE { " +
                    " GRAPH ?del { ?s <" + targetProperty + "> ?o } . " +
                    " ?corrRev <http://wikiba.se/history/ontology#deletions> ?del ; " +
                    "      <http://wikiba.se/history/ontology#additions> ?add ; " +
                    "      <http://wikiba.se/history/ontology#previousRevision>/<http://wikiba.se/history/ontology#globalState> ?global . " +
                    " FILTER NOT EXISTS { " + typesToHaveFilter +
                    "    GRAPH ?global { ?o " + relationsToHave + "/<" + Vocabulary.toDirectProperty(SUBCLASSOF_PROPERTY) + ">* ?o2 } " +
                    " } " +
                    " FILTER NOT EXISTS { GRAPH ?add { ?s <" + OWL.SAMEAS + "> ?red } } " +
                    "}",
            "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) (true AS ?isCorrAddition) (?o AS ?corrS) ?corrP (?o2 AS ?corrO) ?corrRev WHERE { " +
                    " {SELECT DISTINCT ?o WHERE { GRAPH ?g { ?s <" + targetProperty + "> ?o }}}" + //Hack to only work on interesting items
                    " GRAPH ?add { ?o " + relationsToHave + " ?type } . " +
                    " ?corrRev <http://wikiba.se/history/ontology#additions> ?add ; " +
                    "      <http://wikiba.se/history/ontology#deletions> ?del ; " +
                    "      <http://wikiba.se/history/ontology#previousRevision>/<http://wikiba.se/history/ontology#globalState> ?global . " +
                    " GRAPH ?global {" +
                    "           ?s <" + targetProperty + "> ?o ." +
                    typesToHaveFilter +
                    "   ?type <" + Vocabulary.toDirectProperty(SUBCLASSOF_PROPERTY) + ">* ?o2 . " +
                    "  } " +
                    " FILTER NOT EXISTS { GRAPH ?del { ?s <" + targetProperty + "> ?o . } } " +
                    " GRAPH ?add { ?o ?corrP ?type } " + //Needed because relationToHave my be a union of properties
                    "}"
    );
  }

  @Override
  public String buildViolationQuery(Constraint constraint, IRI revision) {
    IRI targetProperty = Vocabulary.toDirectProperty(constraint.getProperty());
    String relationsToHave = convertRelationParameter(constraint);
    String typesToHaveFilter = convertClassParameter(constraint, "o2");

    return "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) FROM <" + Vocabulary.toGlobalState(revision) + "> WHERE { " +
            " ?s <" + targetProperty + "> ?o . " +
            " FILTER NOT EXISTS { " + typesToHaveFilter + " ?o " + relationsToHave + "/<" + Vocabulary.toDirectProperty(SUBCLASSOF_PROPERTY) + ">* ?o2 } " +
            "}";
  }
}
