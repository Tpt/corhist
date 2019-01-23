package org.wikidata.history.corhist;

import org.eclipse.rdf4j.common.iterator.CloseableIterationIterator;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IterableTupleQuery implements Iterable<BindingSet>, AutoCloseable {
  private TupleQuery query;
  private List<AutoCloseable> toClose = new ArrayList<>();

  public IterableTupleQuery(Repository repository, String queryStr) {
    RepositoryConnection connection = repository.getConnection();
    query = connection.prepareTupleQuery(queryStr);
    toClose.add(connection);
  }

  public IterableTupleQuery(RepositoryConnection connection, String queryStr) {
    query = connection.prepareTupleQuery(queryStr);
  }

  public IterableTupleQuery(TupleQuery query) {
    this.query = query;
  }

  @Override
  public Iterator<BindingSet> iterator() {
    TupleQueryResult result = query.evaluate();
    toClose.add(result);

    return new CloseableIterationIterator<>(result);
  }

  public Stream<BindingSet> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  @Override
  public void close() {
    toClose.forEach(e -> {
      try {
        e.close();
      } catch (Exception ex) {

        throw new RuntimeException(ex);
      }
    });
  }
}

