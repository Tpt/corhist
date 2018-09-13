package org.wikidata.history.dataset.queries;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.dataset.Constraint;
import org.wikidata.history.sparql.Vocabulary;

import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * https://www.wikidata.org/wiki/Help:Property_constraints_portal/One_of
 */
public class FormatQueriesBuilder extends AbstractQueriesBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(FormatQueriesBuilder.class);
  private static final IRI TARGET_CONSTRAINT = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q21502404");

  @Override
  public boolean canBuildForConstraint(Constraint constraint) {
    return constraint.getType().equals(TARGET_CONSTRAINT);
  }

  @Override
  public List<String> buildCorrectionsLookupQueries(Constraint constraint) {
    IRI targetProperty = Vocabulary.toDirectProperty(constraint.getProperty());

    return Collections.singletonList(
            "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) (false AS ?isCorrAddition) (?s AS ?corrS) (?o AS ?corrO) ?corrRev WHERE { " +
                    " GRAPH ?del { ?s <" + targetProperty + "> ?o } . " +
                    " FILTER( !REGEX( ?o, \"" + convertRegexParameter(constraint) + "\" ) ) " +
                    " ?corrRev <http://wikiba.se/history/ontology#deletions> ?del ; " +
                    "      <http://wikiba.se/history/ontology#additions> ?add . " +
                    " FILTER NOT EXISTS { GRAPH ?add { ?s <" + OWL.SAMEAS + "> ?red } } " +
                    "}"
    );
  }

  @Override
  public String buildViolationQuery(Constraint constraint, IRI revision) {
    IRI targetProperty = Vocabulary.toDirectProperty(constraint.getProperty());

    return "SELECT DISTINCT (?s AS ?targetS) (?o AS ?targetO) FROM <" + Vocabulary.toGlobalState(revision) + "> WHERE { " +
            " ?s <" + targetProperty + "> ?o . " +
            " FILTER( !REGEX( ?o, \"" + convertRegexParameter(constraint) + "\" ) ) " +
            "}";
  }

  String convertRegexParameter(Constraint constraint) {
    return constraint.getParameters(REGEX_PARAMETER).stream()
            .map(Value::stringValue)
            .map(regex -> "^" + regex + "$")
            .filter(regex -> {
              try {
                Pattern.compile(regex);
                return true;
              } catch (PatternSyntaxException e) {
                LOGGER.info("Invalid regex: " + e.getMessage());
                return false;
              }
            })
            .findAny()
            .map(NTriplesUtil::escapeString)
            .orElseGet(() -> {
              LOGGER.info("No valid regex found for constraint " + constraint);
              return "^.*$";
            });
  }
}
