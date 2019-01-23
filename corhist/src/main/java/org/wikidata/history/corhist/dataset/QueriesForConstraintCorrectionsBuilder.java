package org.wikidata.history.corhist.dataset;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.util.List;

public interface QueriesForConstraintCorrectionsBuilder {

  IRI ITEM_PARAMETER = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/P2305");
  IRI PROPERTY_PARAMETER = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/P2306");
  IRI CLASS_PARAMETER = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/P2308");
  IRI RELATION_PARAMETER = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/P2309");
  IRI REGEX_PARAMETER = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/P1793");

  IRI INSTANCEOF_PROPERTY = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/P31");
  IRI INSTANCEOF_ENTITY = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q21503252");
  IRI SUBCLASSOF_PROPERTY = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/P279");
  IRI SUBCLASSOF_ENTITY = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q21514624");
  IRI INSTANCE_OR_SUBCLASS_OF_PROPERTY = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/Q30208840");
  IRI NOT_EXISTING_PROPERTY = SimpleValueFactory.getInstance().createIRI("http://www.wikidata.org/entity/P1");

  /**
   * Returns true if the object could build queries for the given constraint
   */
  boolean canBuildForConstraint(Constraint constraint);

  /**
   * Returns queries looking for contraint corrections in Wikidata history
   * They should start with "SELECT ?targetS ?targetO ?isCorrAddition ?corrS ?corrO ?corrRev WHERE"
   * You could also return ?corrP for the property used by the correction (by default the constraint property)
   */
  default List<String> buildCorrectionsLookupQueries(Constraint constraint) {
    return buildCorrectionsLookupQueries(constraint, 0);
  }

  default List<String> buildCorrectionsLookupQueries(Constraint constraint, long numberOfInstances) {
    return buildCorrectionsLookupQueries(constraint);
  }


  /**
   * Returns a query looking for constraint violations at a specific revision.
   * They should start with "SELECT ?targetS ?targetO WHERE"
   */
  String buildViolationQuery(Constraint constraint, IRI revision);
}
