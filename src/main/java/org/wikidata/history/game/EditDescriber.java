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
import org.wikidata.history.sparql.Vocabulary;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.helpers.DatamodelMapper;
import org.wikidata.wdtk.datamodel.implementation.StatementImpl;
import org.wikidata.wdtk.datamodel.implementation.ValueImpl;
import org.wikidata.wdtk.datamodel.interfaces.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
            formatValue(Datamodel.makeWikidataItemIdValue(edit.get("entity")), null) + ", " +
            formatValue(Datamodel.makeWikidataPropertyIdValue(edit.get("property")), null) + ", " +
            formatValue(edit.get("value"), edit.get("property")) + ")";
  }

  private String wbremoveclaimsToString(Map<String, String> edit) {
    return Arrays.stream(edit.get("claim").split("\\|"))
            .map(guid -> "Remove statement <a href='" + Vocabulary.WDS_NAMESPACE + guid + "'>" + guid + "</a> :" +
                    getStatement(guid).map(this::formatStatement).orElse("statement not found")
            )
            .collect(Collectors.joining("\n"));
  }

  private String wbsetclaimvalueToString(Map<String, String> edit) {
    return "Edit statement <a href='" + Vocabulary.WDS_NAMESPACE + edit.get("claim") + "'>" + edit.get("claim") + "</a>: " +
            getStatement(edit.get("claim")).map(statement ->
                    formatStatement(statement) + ". Setting value to: " + formatValue(edit.get("value"), statement.getClaim().getMainSnak().getPropertyId().getId())
            ).orElse(
                    "statement not found. Setting value to: " + formatValue(edit.get("value"), null)
            );
  }

  private Optional<Statement> getStatement(String guid) {
    EntityIdValue subjectId = Datamodel.makeWikidataItemIdValue(guid.substring(0, guid.indexOf('$')).toUpperCase());

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
            formatValue(mainSnak.getPropertyId(), null) +
            (mainSnak instanceof ValueSnak ? formatValue(((ValueSnak) mainSnak).getValue(), mainSnak.getPropertyId().getId()) : "?") +
            ")";

  }

  private String formatValue(Value value, String propertyId) {
    try {
      Map<String, String> params = new TreeMap<>();
      params.put("action", "wbformatvalue");
      params.put("generate", "text/html");
      params.put("datavalue", OBJECT_MAPPER.writeValueAsString(value));
      if (propertyId != null) {
        params.put("property", propertyId);
      }
      return apiCall(params).map(result -> result.get("result").textValue()).orElseGet(value::toString);
    } catch (JsonProcessingException e) {
      LOGGER.error(e.getMessage(), e);
      return value.toString();
    }
  }

  private String formatValue(String value, String propertyId) {
    try {
      return formatValue(OBJECT_MAPPER.readValue(value, ValueImpl.class), propertyId);
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
      return value;
    }
  }

  private Optional<JsonNode> apiCall(Map<String, String> params) {
    try {
      HttpUrl.Builder urlBuilder = HttpUrl.parse("https://www.wikidata.org/w/api.php").newBuilder();
      urlBuilder.addQueryParameter("format", "json");
      for (Map.Entry<String, String> param : params.entrySet()) {
        urlBuilder.addQueryParameter(param.getKey(), param.getValue());
      }
      Request request = new Request.Builder().url(urlBuilder.build()).build();
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
}
