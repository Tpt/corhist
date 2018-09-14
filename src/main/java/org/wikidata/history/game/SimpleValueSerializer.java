package org.wikidata.history.game;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.wikidata.wdtk.datamodel.helpers.DataFormatter;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.interfaces.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

class SimpleValueSerializer {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static Object serialize(Value value) {
    if (value instanceof EntityIdValue) {
      return Collections.singletonMap("id", ((EntityIdValue) value).getId());
    }
    if (value instanceof StringValue) {
      return Optional.of(((StringValue) value).getString());
    }
    if (value instanceof MonolingualTextValue) {
      Map<String, Object> serialization = new TreeMap<>();
      serialization.put("language", ((MonolingualTextValue) value).getLanguageCode());
      serialization.put("text", ((MonolingualTextValue) value).getText());
      return serialization;
    }
    if (value instanceof TimeValue) {
      Map<String, Object> serialization = new TreeMap<>();
      serialization.put("time", DataFormatter.formatTimeISO8601((TimeValue) value));
      serialization.put("timezone", ((TimeValue) value).getTimezoneOffset());
      serialization.put("before", ((TimeValue) value).getBeforeTolerance());
      serialization.put("after", ((TimeValue) value).getAfterTolerance());
      serialization.put("precision", ((TimeValue) value).getPrecision());
      serialization.put("calendarmodel", ((TimeValue) value).getPreferredCalendarModel());
      return serialization;
    }
    if (value instanceof GlobeCoordinatesValue) {
      Map<String, Object> serialization = new TreeMap<>();
      serialization.put("latitude", ((GlobeCoordinatesValue) value).getLatitude());
      serialization.put("longitude", ((GlobeCoordinatesValue) value).getLongitude());
      serialization.put("precision", ((GlobeCoordinatesValue) value).getPrecision());
      serialization.put("globe", ((GlobeCoordinatesValue) value).getGlobe());
      return serialization;
    }
    if (value instanceof QuantityValue) {
      Map<String, Object> serialization = new TreeMap<>();
      serialization.put("amount", DataFormatter.formatBigDecimal(((QuantityValue) value).getNumericValue()));
      if (((QuantityValue) value).getUpperBound() != null) {
        serialization.put("upperBound", DataFormatter.formatBigDecimal(((QuantityValue) value).getUpperBound()));
      }
      if (((QuantityValue) value).getLowerBound() != null) {
        serialization.put("lowerBound", DataFormatter.formatBigDecimal(((QuantityValue) value).getLowerBound()));
      }
      serialization.put("unit", ((QuantityValue) value).getUnit());
      return serialization;
    }
    throw new IllegalArgumentException("Not supported value type: " + value);
  }

  static Value deserialize(String json) {
    try {
      return deserialize(OBJECT_MAPPER.readTree(json));
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid value JSON serialization: " + json);
    }
  }

  static Value deserialize(JsonNode tree) {
    if (tree.isTextual()) {
      return Datamodel.makeStringValue(tree.asText());
    }
    if (tree.has("id")) {
      return parseEntityId(tree.get("id").asText());
    }
    //TODO entity-type + numeric-id
    if (tree.has("language") && tree.has("text")) {
      return Datamodel.makeMonolingualTextValue(tree.get("text").asText(), tree.get("language").asText());
    }
    if (tree.has("time")) {
      String time = tree.get("time").asText();
      String[] substrings = time.split("(?<!\\A)[\\-:TZ]");
      return Datamodel.makeTimeValue(
              Long.parseLong(substrings[0]),
              Byte.parseByte(substrings[1]),
              Byte.parseByte(substrings[2]),
              Byte.parseByte(substrings[3]),
              Byte.parseByte(substrings[4]),
              Byte.parseByte(substrings[5]),
              (byte) tree.get("precision").shortValue(),
              tree.get("before").intValue(),
              tree.get("after").intValue(),
              tree.get("timezone").intValue(),
              tree.get("calendarmodel").textValue()
      );
    }
    if (tree.has("latitude") && tree.has("longitude")) {
      return Datamodel.makeGlobeCoordinatesValue(
              tree.get("latitude").doubleValue(),
              tree.get("longitude").doubleValue(),
              tree.get("precision").doubleValue(),
              tree.get("globe").textValue()
      );
    }
    if (tree.has("amount")) {
      return Datamodel.makeQuantityValue(
              new BigDecimal(tree.get("amount").textValue()),
              tree.has("lowerBound") ? new BigDecimal(tree.get("lowerBound").textValue()) : null,
              tree.has("upperBound") ? new BigDecimal(tree.get("upperBound").textValue()) : null,
              tree.get("unit").textValue()
      );
    }
    throw new IllegalArgumentException("Not supported value serialization: " + tree.toString());
  }


  static EntityIdValue parseEntityId(String entityId) {
    switch (entityId.charAt(0)) {
      case 'L':
        return Datamodel.makeWikidataLexemeIdValue(entityId);
      case 'P':
        return Datamodel.makeWikidataPropertyIdValue(entityId);
      case 'Q':
        return Datamodel.makeWikidataItemIdValue(entityId);
      default:
        throw new IllegalArgumentException("Not supported entity id" + entityId);
    }
  }
}
