package org.wikidata.history.game;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.Constants;
import org.wikidata.history.sparql.Vocabulary;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.helpers.DatamodelMapper;
import org.wikidata.wdtk.datamodel.implementation.StatementImpl;
import org.wikidata.wdtk.datamodel.interfaces.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class EditDescriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(EditDescriber.class);
  private static final ObjectMapper OBJECT_MAPPER = new DatamodelMapper(Datamodel.SITE_WIKIDATA);
  private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
          .connectTimeout(1, TimeUnit.SECONDS)
          .readTimeout(1, TimeUnit.SECONDS)
          .retryOnConnectionFailure(false)
          .build();

  String toString(Map<String, String> edit) {
    switch (edit.get("action")) {
      case "wbcreateclaim":
        return wbcreateclaimToString(edit);
      case "wbremoveclaims":
        return wbremoveclaimsToString(edit);
      case "wbsetclaimvalue":
        return wbsetclaimvalueToString(edit);
      default:
        return edit.toString();
    }
  }

  private String wbcreateclaimToString(Map<String, String> edit) {
    return "Add statement (" +
            formatValue(SimpleValueSerializer.parseEntityId(edit.get("entity")), null) + ", " +
            formatValue(Datamodel.makeWikidataPropertyIdValue(edit.get("property")), null) + ", " +
            formatValue(edit.get("value"), edit.get("property")) + ")";
  }

  private String wbremoveclaimsToString(Map<String, String> edit) {
    return Arrays.stream(edit.get("claim").split("\\|"))
            .map(guid -> "Remove <a href='" + Vocabulary.WDS_NAMESPACE + guid + "'>statement</a> " +
                    getStatement(guid).map(this::formatStatement).orElseThrow(StatementNotFoundException::new)
            )
            .collect(Collectors.joining("\n"));
  }

  private String wbsetclaimvalueToString(Map<String, String> edit) {
    return "Edit <a href='" + Vocabulary.WDS_NAMESPACE + edit.get("claim") + "'>statement</a> " +
            getStatement(edit.get("claim")).map(statement ->
                    formatStatement(statement) + ". Setting value to: " + formatValue(edit.get("value"), statement.getClaim().getMainSnak().getPropertyId().getId())
            ).orElseThrow(StatementNotFoundException::new);
  }

  private Optional<Statement> getStatement(String guid) {
    EntityIdValue subjectId = SimpleValueSerializer.parseEntityId(guid.substring(0, guid.indexOf('$')).toUpperCase());

    Map<String, String> params = new TreeMap<>();
    params.put("action", "wbgetclaims");
    params.put("claim", guid);
    return apiCall(params).flatMap(result -> {
      for (JsonNode claims : result.get("claims")) {
        for (JsonNode claim : claims) {
          try {
            return Optional.of(OBJECT_MAPPER.readValue(claim.toString(), StatementImpl.PreStatement.class).withSubject(subjectId));
          } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
          }
        }
      }
      return Optional.empty();
    });
  }

  private String formatStatement(Statement statement) {
    Claim claim = statement.getClaim();
    Snak mainSnak = statement.getClaim().getMainSnak();
    return "(" +
            formatValue(claim.getSubject(), null) + ", " +
            formatValue(mainSnak.getPropertyId(), null) + ", " +
            (mainSnak instanceof ValueSnak ? formatValue(((ValueSnak) mainSnak).getValue(), mainSnak.getPropertyId().getId()) : "?") +
            ")";

  }

  private String formatValue(Value value, String propertyId, String generate) {
    try {
      Map<String, String> params = new TreeMap<>();
      params.put("action", "wbformatvalue");
      params.put("generate", generate);
      params.put("datavalue", OBJECT_MAPPER.writeValueAsString(value));
      if (propertyId != null) {
        params.put("property", propertyId);
      }
      return apiCall(params).map(result -> makeWikidataLinksAbsolute(result.get("result").textValue())).orElseGet(value::toString);
    } catch (JsonProcessingException e) {
      LOGGER.error(e.getMessage(), e);
      return value.toString();
    }
  }

  static String makeWikidataLinksAbsolute(String html) {
    return html.replace("href=\"/wiki/", "href=\"https://www.wikidata.org/wiki/");
  }

  private String formatValue(Value value, String propertyId) {
    return formatValue(value, propertyId, "text/html");
  }

  String formatValueAsText(Value value) {
    return formatValue(value, null, "text/plain");
  }

  private String formatValue(String value, String propertyId, String generate) {
    try {
      return formatValue(SimpleValueSerializer.deserialize(value), propertyId, generate);
    } catch (IllegalArgumentException e) {
      LOGGER.error(e.getMessage(), e);
      return value;
    }
  }

  private String formatValue(String value, String propertyId) {
    return formatValue(value, propertyId, "text/html");
  }

  private Optional<JsonNode> apiCall(Map<String, String> params) {
    try {
      HttpUrl.Builder urlBuilder = HttpUrl.parse("https://www.wikidata.org/w/api.php").newBuilder();
      urlBuilder.addQueryParameter("format", "json");
      for (Map.Entry<String, String> param : params.entrySet()) {
        urlBuilder.addQueryParameter(param.getKey(), param.getValue());
      }
      Request request = new Request.Builder()
              .url(urlBuilder.build())
              .addHeader("User-Agent", Constants.USER_AGENT)
              .build();
      try (Response response = CLIENT.newCall(request).execute()) {
        if (response.body() == null) {
          LOGGER.error("No response : " + response);
          return Optional.empty();
        }
        try (InputStream inputStream = response.body().byteStream()) {
          return Optional.of(OBJECT_MAPPER.readTree(inputStream));
        }
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Optional.empty();
    }
  }

  Stream<String> entities(Map<String, String> edit) {
    return extractEntities(edit).map(EntityIdValue::getId).distinct();
  }

  private Stream<EntityIdValue> extractEntities(Map<String, String> edit) {
    switch (edit.get("action")) {
      case "wbcreateclaim":
        return wbcreateclaimEntities(edit);
      case "wbremoveclaims":
        return wbremoveclaimsEntities(edit);
      case "wbsetclaimvalue":
        return wbsetclaimvalueEntities(edit);
      default:
        return Stream.empty();
    }
  }

  private Stream<EntityIdValue> wbcreateclaimEntities(Map<String, String> edit) {
    return Stream.concat(
            Stream.of(SimpleValueSerializer.parseEntityId(edit.get("entity"))),
            valueEntities(edit.get("value"))

    );
  }

  private Stream<EntityIdValue> wbremoveclaimsEntities(Map<String, String> edit) {
    return Arrays.stream(edit.get("claim").split("\\|"))
            .flatMap(guid -> getStatement(guid).map(Stream::of).orElseGet(Stream::empty))
            .flatMap(this::statementEntities);
  }

  private Stream<EntityIdValue> wbsetclaimvalueEntities(Map<String, String> edit) {
    return Stream.concat(
            getStatement(edit.get("claim")).map(Stream::of).orElseGet(Stream::empty).flatMap(this::statementEntities),
            valueEntities(edit.get("value"))
    );
  }

  private Stream<EntityIdValue> statementEntities(Statement statement) {
    return Stream.concat(
            Stream.of(statement.getSubject()),
            Optional.of(statement.getValue())
                    .flatMap(value -> value instanceof EntityIdValue ? Optional.of((EntityIdValue) value) : Optional.empty())
                    .map(Stream::of).orElseGet(Stream::empty)
    );
  }

  private Stream<EntityIdValue> valueEntities(String valueString) {
    try {
      Value value = SimpleValueSerializer.deserialize(valueString);
      if (value instanceof EntityIdValue) {
        return Stream.of((EntityIdValue) value);
      } else {
        return Stream.empty();
      }
    } catch (IllegalArgumentException e) {
      LOGGER.error(e.getMessage(), e);
      return Stream.empty();
    }
  }
}
