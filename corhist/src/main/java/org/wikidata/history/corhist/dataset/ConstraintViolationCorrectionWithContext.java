package org.wikidata.history.corhist.dataset;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.wikidata.history.sparql.Vocabulary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class ConstraintViolationCorrectionWithContext {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
  private static final IRI HISTORY_PAGE_STATUS_CODE = VALUE_FACTORY.createIRI(Vocabulary.WBHISTORY_NAMESPACE, "pageStatusCode");

  private ConstraintViolationCorrection correction;
  private EntityDescription subjectContext;
  private ContextElement objectContext;
  private ContextElement otherObjectContext;
  private Map<IRI, List<Map.Entry<Value, Value>>> psoContext;

  ConstraintViolationCorrectionWithContext(ConstraintViolationCorrection correction, EntityDescription subjectContext, ContextElement objectContext, ContextElement otherObjectContext) {
    this.correction = correction;
    this.subjectContext = subjectContext;
    this.objectContext = objectContext;
    this.otherObjectContext = otherObjectContext;
  }

  public static ConstraintViolationCorrectionWithContext read(String line, ValueFactory valueFactory, Map<IRI, Constraint> constraints) {
    String[] parts = charSplit(line, '\t');
    if (parts.length < 10) {
      throw new IllegalArgumentException("Invalid correction serialization: " + line);
    }
    IRI constraintIRI = NTriplesUtil.parseURI(parts[0], valueFactory);
    if (!constraints.containsKey(constraintIRI)) {
      return null;
    }
    EntityDescription subjectDesc = (EntityDescription) readContext(parts[parts.length - 3]);
    ContextElement objectDec = readContext(parts[parts.length - 2]);
    ContextElement otherObjectDec = readContext(parts[parts.length - 1]);
    return new ConstraintViolationCorrectionWithContext(new ConstraintViolationCorrection(
            constraints.get(constraintIRI),
            valueFactory.createStatement(
                    NTriplesUtil.parseResource(parts[2], valueFactory),
                    NTriplesUtil.parseURI(parts[3], valueFactory),
                    NTriplesUtil.parseValue(parts[4], valueFactory)
            ),
            (parts[5].equals("")) ? null : valueFactory.createStatement(
                    NTriplesUtil.parseResource(parts[5], valueFactory),
                    NTriplesUtil.parseURI(parts[6], valueFactory),
                    NTriplesUtil.parseValue(parts[7], valueFactory)
            ),
            IntStream.range(0, (parts.length - 9) / 4).mapToObj(i -> valueFactory.createStatement(
                    NTriplesUtil.parseResource(parts[4 * i + 9], valueFactory),
                    NTriplesUtil.parseURI(parts[4 * i + 10], valueFactory),
                    NTriplesUtil.parseValue(parts[4 * i + 11], valueFactory),
                    NTriplesUtil.parseResource(parts[4 * i + 12], valueFactory)
            )).collect(Collectors.toSet()),
            NTriplesUtil.parseURI(parts[1], valueFactory)
    ), subjectDesc, objectDec, otherObjectDec);
  }

  private static ContextElement readContext(String value) {
    value = value.trim();
    if (value.isEmpty()) {
      return null;
    }

    try {
      return OBJECT_MAPPER.readerFor(ContextElement.class).readValue(value);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid context element: " + value, e);
    }
  }

  public static String[] charSplit(String str, char ch) {
    ArrayList<String> list = new ArrayList<>();
    int off = 0;
    int next;
    for (; (next = str.indexOf(ch, off)) != -1; off = next + 1) {
      list.add(str.substring(off, next));
    }

    if (off == 0) {
      return new String[]{str};
    } else {
      list.add(str.substring(off));
      return list.toArray(String[]::new);
    }
  }

  public ConstraintViolationCorrection getCorrection() {
    return correction;
  }

  public Map<IRI, List<Map.Entry<Value, Value>>> getPsoContext() {
    if (psoContext == null) {
      psoContext = new HashMap<>();
      addContextObjectToContext(subjectContext, correction.getTargetTriple());
      addContextObjectToContext(objectContext, correction.getTargetTriple());
      if (correction.getOtherTargetTriple() != null) {
        Statement otherTriple = correction.getOtherTargetTriple();
        addToPsoContext(otherTriple.getSubject(), otherTriple.getPredicate(), otherTriple.getObject());
        addContextObjectToContext(otherObjectContext, otherTriple);
      }
    }
    return psoContext;
  }

  private void addContextObjectToContext(ContextElement entity, Statement context) {
    if (entity instanceof EntityDescription) {
      addEntityToContext((EntityDescription) entity, context);
    } else if (entity instanceof WebPage) {
      addWebPageToContext((WebPage) entity, context);
    }
  }

  private void addEntityToContext(EntityDescription entity, Statement context) {
    if (entity != null) {
      for (Map.Entry<IRI, List<Value>> facts : entity.getFacts().entrySet()) {
        for (Value value : facts.getValue()) {
          addToPsoContext(entity.getId(), facts.getKey(), value);
        }
      }
    }
  }

  private void addWebPageToContext(WebPage entity, Statement context) {
    addToPsoContext(context.getObject(), HISTORY_PAGE_STATUS_CODE, VALUE_FACTORY.createLiteral(entity.getStatusCode()));
  }

  private void addToPsoContext(Value subject, IRI predicate, Value object) {
    psoContext.computeIfAbsent(predicate, k -> new ArrayList<>())
            .add(Map.entry(subject, object));
  }
}
