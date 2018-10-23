package org.wikidata.history.game;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import okhttp3.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.Constants;
import org.wikidata.history.WikidataSPARQLEndpoint;
import org.wikidata.history.sparql.Vocabulary;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

final class ViolationDatabaseUpdater implements AutoCloseable {

  private static final String CONSTRAINT_TYPES = "wd:Q21502838 wd:Q21510859 wd:Q21510865 wd:Q21510855 wd:Q21510862 wd:Q19474404 wd:Q21502410 wd:Q21503247 wd:Q21510864 wd:Q21503250 wd:Q21510865"; //TODO: update
  private static final String QUERY = "SELECT DISTINCT ?entity WHERE {\n" +
          "  VALUES ?constraintType { " + CONSTRAINT_TYPES + " } \n" +
          "  ?statement wikibase:hasViolationForConstraint ?constraint .\n" +
          "  ?constraint ps:P2302 ?constraintType .\n" +
          "  ?entity ?subjectProperty ?statement .\n" +
          "} LIMIT 100000";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(ViolationDatabaseUpdater.class);
  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
  private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
          .connectTimeout(10, TimeUnit.SECONDS)
          .readTimeout(10, TimeUnit.SECONDS)
          .retryOnConnectionFailure(false)
          .build();

  private final Callback newEditDataCallback;
  private EventSource eventSource;

  ViolationDatabaseUpdater(ViolationDatabase violationDatabase, CorrectionLookup correctionLookup) {
    this.newEditDataCallback = new NewEditDataCallback(violationDatabase, correctionLookup);
  }

  void loadFromWikidataQuery() {
    try (WikidataSPARQLEndpoint endpoint = new WikidataSPARQLEndpoint()) {
      endpoint.executeTupleQuery(QUERY).stream().forEach(bindingSet -> {
        IRI entity = (IRI) bindingSet.getValue("entity");
        Request request = new Request.Builder()
                .url("https://www.wikidata.org/wiki/Special:EntityData/" + entity.getLocalName() + ".ttl?flavor=dump")
                .addHeader("User-Agent", Constants.USER_AGENT)
                .build();
        CLIENT.newCall(request).enqueue(newEditDataCallback);
        try {
          Thread.sleep(5000); //We avoid to overload the server
        } catch (InterruptedException e) {
          //We don't care much
        }
      });
    }
  }

  void startToLoadFromRecentChanges() {
    eventSource = (new EventSource.Builder(
            new ViolationEventHandler(newEditDataCallback),
            URI.create("https://stream.wikimedia.org/v2/stream/revision-create"))
    ).build();
    eventSource.start();
  }

  @Override
  public void close() {
    if (eventSource != null) {
      eventSource.close();
    }
  }

  private static final class ViolationEventHandler implements EventHandler {

    private final Callback newEditDataCallback;

    private ViolationEventHandler(Callback newEditDataCallback) {
      this.newEditDataCallback = newEditDataCallback;
    }

    @Override
    public void onOpen() {
    }

    @Override
    public void onClosed() {
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws IOException {
      JsonNode data = OBJECT_MAPPER.readTree(messageEvent.getData());
      if ("wikibase-item".equals(data.get("rev_content_model").textValue()) && "wikidatawiki".equals(data.get("database").textValue()) && data.get("rev_content_changed").asBoolean()) {
        onItemChange(data.get("page_title").textValue());
      }
    }

    private void onItemChange(String itemId) {
      Request request = new Request.Builder()
              .url("https://www.wikidata.org/wiki/Special:EntityData/" + itemId + ".ttl?flavor=dump")
              .addHeader("User-Agent", Constants.USER_AGENT)
              .build();
      CLIENT.newCall(request).enqueue(newEditDataCallback);
    }

    @Override
    public void onComment(String s) {

    }

    @Override
    public void onError(Throwable throwable) {
      LOGGER.warn(throwable.getMessage(), throwable);
    }
  }


  private static final class NewEditDataCallback implements Callback {

    private final ViolationDatabase violationDatabase;
    private final CorrectionLookup correctionLookup;

    private NewEditDataCallback(ViolationDatabase violationDatabase, CorrectionLookup correctionLookup) {
      this.violationDatabase = violationDatabase;
      this.correctionLookup = correctionLookup;
    }

    @Override
    public void onFailure(Call call, IOException e) {
      if (!(e instanceof SocketTimeoutException)) {
        LOGGER.warn(e.getMessage(), e);
      }
    }

    @Override
    public void onResponse(Call call, Response response) throws IOException {
      HttpUrl url = response.request().url();
      String entityId = url.pathSegments().get(url.pathSize() - 1).replace(".ttl", "");
      try (ResponseBody body = response.body()) {
        if (body == null) {
          throw new IOException("Unexpected code " + response);
        }
        try (Reader reader = body.charStream()) {
          Model data = Rio.parse(reader, url.toString(), RDFFormat.TURTLE);
          Request request = new Request.Builder()
                  .url("https://www.wikidata.org/w/api.php?action=wbcheckconstraints&format=json&id=" + entityId)
                  .addHeader("User-Agent", Constants.USER_AGENT)
                  .build();
          CLIENT.newCall(request).enqueue(new WbCheckConstraintsCallback(violationDatabase, correctionLookup, data));
        }
      }
    }
  }

  private static final class WbCheckConstraintsCallback implements Callback {

    private final ViolationDatabase violationDatabase;
    private final CorrectionLookup correctionLookup;
    private final WikidataEditBuilder editBuilder;
    private final Model data;

    private WbCheckConstraintsCallback(ViolationDatabase violationDatabase, CorrectionLookup correctionLookup, Model data) {
      this.violationDatabase = violationDatabase;
      this.data = data;
      this.correctionLookup = correctionLookup;
      this.editBuilder = new WikidataEditBuilder(data);
    }

    @Override
    public void onFailure(Call call, IOException e) {
      if (!(e instanceof SocketTimeoutException)) {
        LOGGER.warn(e.getMessage(), e);
      }
    }

    @Override
    public void onResponse(Call call, Response response) throws IOException {
      try (ResponseBody responseBody = response.body()) {
        if (responseBody == null || !response.isSuccessful()) {
          throw new IOException("Unexpected code " + response);
        }

        try (InputStream inputStream = responseBody.byteStream()) {
          OBJECT_MAPPER.readTree(inputStream).get("wbcheckconstraints").fields().forEachRemaining(entityViolations -> {
            IRI entity = VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, entityViolations.getKey());
            violationDatabase.clearProposedViolations(entity.getLocalName());
            entityViolations.getValue().get("claims").fields().forEachRemaining(propertyWithClaims -> {
              IRI property = VALUE_FACTORY.createIRI(Vocabulary.WDT_NAMESPACE, propertyWithClaims.getKey());
              propertyWithClaims.getValue().forEach(v3 -> {
                IRI statement = buildStatementIRI(v3.get("id").textValue());
                v3.get("mainsnak").get("results").forEach(violation -> {
                  String message = violation.get("message-html").textValue();
                  IRI constraint = buildStatementIRI(violation.get("constraint").get("id").textValue());
                  IRI type = VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, violation.get("constraint").get("type").textValue());
                  correctionLookup.buildPossibleCorrection(constraint, entity, property, statement, data)
                          .flatMap(editBuilder::buildEdit)
                          .ifPresent(correction ->
                                  violationDatabase.addViolation(
                                          entity.getLocalName(),
                                          property.getLocalName(),
                                          statement.getLocalName(),
                                          constraint.getLocalName(),
                                          type.getLocalName(),
                                          message,
                                          correction
                                  )
                          );
                });
              });
            });
          });
        }
      }
    }

    private static IRI buildStatementIRI(String id) {
      return VALUE_FACTORY.createIRI(Vocabulary.WDS_NAMESPACE, id.replace('$', '-'));
    }
  }
}
