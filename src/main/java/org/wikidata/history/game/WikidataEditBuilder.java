package org.wikidata.history.game;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.wikidata.history.sparql.Vocabulary;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.interfaces.TimeValue;

import javax.xml.datatype.XMLGregorianCalendar;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TODO: expend
 */
class WikidataEditBuilder {

  private static final String SUMMARY = "Test of automated constraint violation correction by [[User:Tpt]]";
  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Model model;

  WikidataEditBuilder(Model model) {
    this.model = model;
  }

  Optional<Map<String, String>> buildEdit(Set<Statement> diff) {
    switch (diff.size()) {
      case 1:
        return oneStatementCase(diff.iterator().next());
      case 2:
        return twoStatementsCase(diff);
      default:
        return Optional.empty(); //TODO
    }
  }

  private Optional<Map<String, String>> oneStatementCase(Statement statement) {
    Resource subject = statement.getSubject();
    IRI predicate = statement.getPredicate();
    Value object = statement.getObject();
    Resource context = statement.getContext();
    if (!(subject instanceof IRI) || !((IRI) subject).getNamespace().equals(Vocabulary.WD_NAMESPACE) ||
            !predicate.getNamespace().equals(Vocabulary.WDT_NAMESPACE)) {
      return Optional.empty();
    }
    if (Vocabulary.HISTORY_DELETION.equals(context)) {
      return oneDeletionCase(((IRI) subject).getLocalName(), predicate.getLocalName(), object);
    } else if (Vocabulary.HISTORY_ADDITION.equals(context)) {
      return oneAdditionCase(((IRI) subject).getLocalName(), predicate.getLocalName(), object);
    } else {
      return Optional.empty();
    }
  }

  private Optional<Map<String, String>> oneAdditionCase(String entityId, String propertyId, Value value) {
    return convertDataValue(value).flatMap(WikidataEditBuilder::valueToSerialize).map(valueO -> {
      try {
        Map<String, String> edit = new TreeMap<>();
        edit.put("action", "wbcreateclaim");
        edit.put("entity", entityId);
        edit.put("snaktype", "value");
        edit.put("property", propertyId);
        edit.put("value", OBJECT_MAPPER.writeValueAsString(valueO));
        edit.put("summary", SUMMARY);
        return edit;
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private Optional<Map<String, String>> oneDeletionCase(String entityId, String propertyId, Value value) {
    String guids = getGuids(entityId, propertyId, value).collect(Collectors.joining("|"));
    if (guids.isEmpty()) {
      return Optional.empty();
    }
    Map<String, String> edit = new TreeMap<>();
    edit.put("action", "wbremoveclaims");
    edit.put("claim", getGuids(entityId, propertyId, value).collect(Collectors.joining("|")));
    edit.put("summary", SUMMARY);
    return Optional.of(edit);
  }

  private Optional<Map<String, String>> twoStatementsCase(Set<Statement> statements) {
    Statement addition = null;
    Statement deletion = null;
    for (Statement s : statements) {
      Resource subject = s.getSubject();
      IRI predicate = s.getPredicate();
      Resource context = s.getContext();
      if (!(subject instanceof IRI) || !((IRI) subject).getNamespace().equals(Vocabulary.WD_NAMESPACE) || !predicate.getNamespace().equals(Vocabulary.WDT_NAMESPACE)) {
        return Optional.empty();
      }
      if (Vocabulary.HISTORY_DELETION.equals(context) && deletion == null) {
        deletion = s;
      } else if (Vocabulary.HISTORY_ADDITION.equals(context) && addition == null) {
        addition = s;
      } else {
        return Optional.empty();
      }
    }
    if (addition.getSubject().equals(deletion.getSubject()) && addition.getPredicate().equals(deletion.getPredicate()) && !addition.getObject().equals(deletion.getObject())) {
      return oneReplacementCase(((IRI) addition.getSubject()).getLocalName(), addition.getPredicate().getLocalName(), deletion.getObject(), addition.getObject());
    }
    return Optional.empty();
  }

  private Optional<Map<String, String>> oneReplacementCase(String entityId, String propertyId, Value fromValue, Value toValue) {
    return getGuids(entityId, propertyId, fromValue).flatMap(guid -> convertDataValue(toValue).flatMap(WikidataEditBuilder::valueToSerialize).map(value -> {
      try {
        Map<String, String> edit = new TreeMap<>();
        edit.put("action", "wbsetclaimvalue");
        edit.put("claim", guid);
        edit.put("snaktype", "value");
        edit.put("value", OBJECT_MAPPER.writeValueAsString(value));
        edit.put("summary", SUMMARY);
        return edit;
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }).map(Stream::of).orElseGet(Stream::empty)).findAny();
  }

  private Stream<String> getGuids(String subject, String propertyId, Value object) {
    return model.filter(VALUE_FACTORY.createIRI(Vocabulary.WD_NAMESPACE, subject), VALUE_FACTORY.createIRI(Vocabulary.P_NAMESPACE, propertyId), null)
            .objects()
            .stream()
            .flatMap(statement -> model.filter((Resource) statement, VALUE_FACTORY.createIRI(Vocabulary.PS_NAMESPACE, propertyId), object).subjects().stream())
            .map(statement -> ((IRI) statement).getLocalName().replaceFirst("-", "\\$"))
            .distinct();
  }

  private Optional<org.wikidata.wdtk.datamodel.interfaces.Value> convertDataValue(Value value) {
    if (value instanceof IRI) {
      return convertDataValue((IRI) value);
    } else if (value instanceof Literal) {
      return convertDataValue((Literal) value);
    } else {
      return Optional.empty();
    }
  }

  private Optional<org.wikidata.wdtk.datamodel.interfaces.Value> convertDataValue(IRI value) {
    switch (value.getNamespace()) {
      case Vocabulary.WD_NAMESPACE:
        String id = value.getLocalName();
        switch (id.charAt(0)) {
          case 'Q':
            return Optional.of(Datamodel.makeWikidataItemIdValue(id));
          case 'P':
            return Optional.of(Datamodel.makeWikidataPropertyIdValue(id));
          default:
            return Optional.empty();
        }
      default:
        return Optional.empty();
    }
  }

  private Optional<org.wikidata.wdtk.datamodel.interfaces.Value> convertDataValue(Literal value) {
    IRI datatype = value.getDatatype();
    if (datatype.equals(XMLSchema.STRING)) {
      return Optional.of(Datamodel.makeStringValue(value.getLabel()));
    } else if (datatype.equals(RDF.LANGSTRING)) {
      return value.getLanguage().map(language -> Datamodel.makeMonolingualTextValue(value.getLabel(), language));
    } else if (datatype.equals(XMLSchema.DATETIME)) {
      try {
        XMLGregorianCalendar calendar = value.calendarValue();
        return Optional.of(Datamodel.makeTimeValue(
                calendar.getEonAndYear().longValueExact(),
                (byte) calendar.getMonth(),
                (byte) calendar.getDay(),
                (byte) calendar.getHour(),
                (byte) calendar.getMinute(),
                (byte) calendar.getSecond(),
                (byte) calendar.getTimezone(),
                TimeValue.CM_GREGORIAN_PRO
        ));
      } catch (IllegalArgumentException | ArithmeticException e) {
        return Optional.empty();
      }
    } else {
      return Optional.empty();
    }
  }

  private static Optional<Object> valueToSerialize(org.wikidata.wdtk.datamodel.interfaces.Value value) {
    try {
      return Optional.of(SimpleValueSerializer.serialize(value));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
