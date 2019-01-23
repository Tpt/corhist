package org.wikidata.history.corhist;

import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

import java.util.Collections;

public class WikidataSPARQLEndpoint implements AutoCloseable {

  private static final String WDQS_ENDPOINT = "https://query.wikidata.org/sparql";

  private SPARQLRepository repo;

  public WikidataSPARQLEndpoint() {
    repo = new SPARQLRepository(WDQS_ENDPOINT);
    repo.setAdditionalHttpHeaders(Collections.singletonMap("User-Agent", Constants.USER_AGENT));
    repo.initialize();
  }

  public IterableTupleQuery executeTupleQuery(String query) {
    return new IterableTupleQuery(repo, query);
  }

  @Override
  public void close() {
    repo.shutDown();
  }
}
