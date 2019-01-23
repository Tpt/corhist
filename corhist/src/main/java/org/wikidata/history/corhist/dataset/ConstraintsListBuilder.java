package org.wikidata.history.corhist.dataset;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;
import org.wikidata.history.corhist.IterableTupleQuery;
import org.wikidata.history.corhist.WikidataSPARQLEndpoint;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ConstraintsListBuilder {
  private static final String QUERY = "SELECT DISTINCT ?constraint ?property ?type ?qualifierProp ?qualifierValue WHERE {"
          + " ?property a wikibase:Property . "
          + " ?property p:P2302 ?constraint . "
          + " ?constraint ps:P2302 ?type . "
          + " ?constraint a wikibase:BestRank . "
          + " OPTIONAL { ?constraint ?qualifierRel ?qualifierValue . ?qualifierProp wikibase:qualifier ?qualifierRel. }"
          + "}";

  public Collection<Constraint> build() {
    try (WikidataSPARQLEndpoint endpoint = new WikidataSPARQLEndpoint()) {
      try (IterableTupleQuery result = endpoint.executeTupleQuery(QUERY)) {
        Map<IRI, Constraint> constraints = new HashMap<>();
        for (BindingSet bindingSet : result) {
          IRI id = (IRI) bindingSet.getValue("constraint");
          IRI property = (IRI) bindingSet.getValue("property");
          IRI type = (IRI) bindingSet.getValue("type");
          Constraint constraint = constraints.computeIfAbsent(id, k -> new Constraint(id, property, type));
          if (bindingSet.hasBinding("qualifierProp") && bindingSet.hasBinding("qualifierValue")) {
            constraint.addParameter((IRI) bindingSet.getValue("qualifierProp"), bindingSet.getValue("qualifierValue"));
          }
        }
        return constraints.values();
      }
    }
  }
}
