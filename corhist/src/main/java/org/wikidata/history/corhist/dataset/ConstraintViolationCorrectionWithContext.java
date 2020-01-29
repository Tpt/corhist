package org.wikidata.history.corhist.dataset;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class ConstraintViolationCorrectionWithContext {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private ConstraintViolationCorrection correction;
  private EntityDescription subjectContext;
  private ContextElement objectContext;

  ConstraintViolationCorrectionWithContext(ConstraintViolationCorrection correction, EntityDescription subjectContext, ContextElement objectContext) {
    this.correction = correction;
    this.subjectContext = subjectContext;
    this.objectContext = objectContext;
  }

  public static ConstraintViolationCorrectionWithContext read(String line, ValueFactory valueFactory, Map<IRI, Constraint> constraints) {
    String[] parts = line.trim().split("\t");
    if (parts.length < 10) {
      throw new IllegalArgumentException("Invalid correction serialization: " + line);
    }
    IRI constraintIRI = NTriplesUtil.parseURI(parts[0], valueFactory);
    if (!constraints.containsKey(constraintIRI)) {
      throw new IllegalArgumentException("Constraint " + constraintIRI + " not found");
    }
    EntityDescription subjectDesc = (parts.length % 4 == 0)
            ? (EntityDescription) readContext(parts[parts.length - 2])
            : (EntityDescription) readContext(parts[parts.length - 1]);
    ContextElement objectDec = (parts.length % 4 == 0)
            ? readContext(parts[parts.length - 1])
            : null;
    return new ConstraintViolationCorrectionWithContext(new ConstraintViolationCorrection(
            constraints.get(constraintIRI),
            valueFactory.createStatement(
                    NTriplesUtil.parseResource(parts[2], valueFactory),
                    NTriplesUtil.parseURI(parts[3], valueFactory),
                    NTriplesUtil.parseValue(parts[4], valueFactory)
            ),
            IntStream.range(0, (parts.length - 6) / 4).mapToObj(i -> valueFactory.createStatement(
                    NTriplesUtil.parseResource(parts[4 * i + 6], valueFactory),
                    NTriplesUtil.parseURI(parts[4 * i + 7], valueFactory),
                    NTriplesUtil.parseValue(parts[4 * i + 8], valueFactory),
                    NTriplesUtil.parseResource(parts[4 * i + 9], valueFactory)
            )).collect(Collectors.toSet()),
            NTriplesUtil.parseURI(parts[1], valueFactory)
    ), subjectDesc, objectDec);
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

  public ConstraintViolationCorrection getCorrection() {
    return correction;
  }

  public EntityDescription getSubjectContext() {
    return subjectContext;
  }

  public ContextElement getObjectContext() {
    return objectContext;
  }


  public Model buildContextModel() {
    Model context = new LinkedHashModel();
    addEntityToContext(subjectContext, context);
    if (objectContext instanceof EntityDescription) {
      addEntityToContext((EntityDescription) objectContext, context);
    } else if (objectContext instanceof WebPage) {
      //TODO
    }
    return context;
  }

  private void addEntityToContext(EntityDescription entity, Model context) {
    if (entity != null) {
      for (Map.Entry<IRI, List<Value>> facts : entity.getFacts().entrySet()) {
        for (Value value : facts.getValue()) {
          context.add(entity.getId(), facts.getKey(), value);
        }
      }
    }
  }
}
