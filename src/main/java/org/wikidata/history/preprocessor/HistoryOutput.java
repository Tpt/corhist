package org.wikidata.history.preprocessor;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

import java.io.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

class HistoryOutput implements AutoCloseable {

  private Writer revisionsWriter;
  private Writer redirectionsWriter;
  private Writer directStatementsWriter;

  HistoryOutput(Path directory) throws IOException {
    revisionsWriter = gzipWriter(directory.resolve("revisions.tsv.gz"));
    redirectionsWriter = gzipWriter(directory.resolve("redirections.tsv.gz"));
    directStatementsWriter = gzipWriter(directory.resolve("direct_statements.tsv.gz"));
  }

  private Writer gzipWriter(Path path) throws IOException {
    return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(path.toFile()))));
  }

  synchronized void addRevision(
          long revisionId, long parentRevisionId, String entityId, Instant timestamp, String contributorName, String comment, String sha1
  ) throws IOException {
    revisionsWriter
            .append(Long.toString(revisionId)).append('\t')
            .append(Long.toString(parentRevisionId)).append('\t')
            .append(entityId).append('\t')
            .append(Long.toString(timestamp.getEpochSecond())).append('\t')
            .append(contributorName).append('\t')
            .append(NTriplesUtil.escapeString(comment)).append('\t')
            .append(sha1).append('\n');
  }

  synchronized void addRedirection(String fromEntity, String toEntity, long revisionId) throws IOException {
    redirectionsWriter.append(fromEntity).append('\t')
            .append(toEntity).append('\t')
            .append(Long.toString(revisionId)).append("\n");
  }

  synchronized void addDirectStatement(Resource subject, IRI predicate, Value object, long... revisionIds) throws IOException {
    directStatementsWriter.append(NTriplesUtil.toNTriplesString(subject)).append('\t')
            .append(NTriplesUtil.toNTriplesString(predicate)).append('\t')
            .append(NTriplesUtil.toNTriplesString(object)).append('\t')
            .append(Arrays.stream(revisionIds).mapToObj(Long::toString).collect(Collectors.joining(" "))).append("\n");
  }

  @Override
  public void close() throws IOException {
    revisionsWriter.close();
    redirectionsWriter.close();
    directStatementsWriter.close();
  }
}
