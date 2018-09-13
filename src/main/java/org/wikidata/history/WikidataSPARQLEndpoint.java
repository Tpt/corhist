package org.wikidata.history;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;

import java.util.Collections;

public class WikidataSPARQLEndpoint implements AutoCloseable {

  private static final String WDQS_ENDPOINT = "https://query.wikidata.org/sparql";

  private SPARQLRepository repo;

  public WikidataSPARQLEndpoint() {
    repo = new SPARQLRepository(WDQS_ENDPOINT);
    repo.setAdditionalHttpHeaders(Collections.singletonMap("User-Agent", "WikidataHistory 0.1; User:Tpt"));
    repo.initialize();
  }

  public RepositoryConnection newConnection() {
    return repo.getConnection();
  }

  public IterableTupleQuery executeTupleQuery(String query) {
    return new IterableTupleQuery(repo, query);
  }

  @Override
  public void close() {
    repo.shutDown();
  }
}
