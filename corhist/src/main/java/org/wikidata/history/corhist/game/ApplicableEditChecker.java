package org.wikidata.history.corhist.game;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.corhist.Constants;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.helpers.DatamodelMapper;
import org.wikidata.wdtk.datamodel.implementation.EntityDocumentImpl;
import org.wikidata.wdtk.datamodel.interfaces.EntityIdValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyIdValue;
import org.wikidata.wdtk.datamodel.interfaces.StatementDocument;
import org.wikidata.wdtk.datamodel.interfaces.Value;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

class ApplicableEditChecker {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicableEditChecker.class);
  private static final ObjectMapper OBJECT_MAPPER = new DatamodelMapper(Datamodel.SITE_WIKIDATA);
  private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
          .connectTimeout(1, TimeUnit.SECONDS)
          .readTimeout(1, TimeUnit.SECONDS)
          .retryOnConnectionFailure(false)
          .build();

  boolean isApplicable(Map<String, String> edit) {
    switch (edit.get("action")) {
      case "wbcreateclaim":
        return isWbcreateclaimApplicable(edit);
      case "wbremoveclaims":
        return isWbremoveclaimsApplicable(edit);
      case "wbsetclaimvalue":
        return isWbsetclaimvalueApplicable(edit);
      default:
        return true;
    }
  }

  private boolean isWbcreateclaimApplicable(Map<String, String> edit) {
    return !hasStatement(
            SimpleValueSerializer.parseEntityId(edit.get("entity")),
            Datamodel.makeWikidataPropertyIdValue(edit.get("property")),
            SimpleValueSerializer.deserialize(edit.get("value"))
    );
  }

  private boolean isWbremoveclaimsApplicable(Map<String, String> edit) {
    return hasStatement(edit.get("claim"));
  }

  private boolean isWbsetclaimvalueApplicable(Map<String, String> edit) {
    return hasStatement(edit.get("claim"));
  }

  private boolean hasStatement(EntityIdValue subject, PropertyIdValue predicate, Value object) {
    Map<String, String> params = new TreeMap<>();
    params.put("action", "wbgetentities");
    params.put("ids", subject.getId());
    params.put("props", "claims");
    return apiCall(params).flatMap(result -> {
      for (JsonNode entity : result.get("entities")) {
        try {
          return Optional.of(OBJECT_MAPPER.readValue(entity.toString(), EntityDocumentImpl.class));
        } catch (IOException e) {
          LOGGER.error(e.getMessage(), e);
        }
      }
      return Optional.empty();
    }).map(entity -> {
      if (entity instanceof StatementDocument) {
        return ((StatementDocument) entity).hasStatementValue(predicate, object);
      } else {
        return false;
      }
    }).orElse(false);
  }

  private boolean hasStatement(String guid) {
    Map<String, String> params = new TreeMap<>();
    params.put("action", "wbgetclaims");
    params.put("claim", guid);
    return apiCall(params).map(result -> result.get("claims").size() > 0).orElse(false);
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
}
