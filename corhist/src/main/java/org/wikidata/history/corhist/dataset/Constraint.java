package org.wikidata.history.corhist.dataset;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public final class Constraint {

  private IRI id;
  private IRI property;
  private IRI type;
  private Map<IRI, List<Value>> parameters;

  public Constraint(IRI id, IRI property, IRI type, Map<IRI, List<Value>> parameters) {
    this.id = id;
    this.property = property;
    this.type = type;
    this.parameters = parameters;
  }

  public Constraint(IRI id, IRI property, IRI type) {
    this(id, property, type, new HashMap<>());
  }

  public IRI getId() {
    return id;
  }

  public IRI getProperty() {
    return property;
  }

  public IRI getType() {
    return type;
  }

  public List<Value> getParameters(IRI property) {
    return parameters.getOrDefault(property, Collections.emptyList());
  }

  public Optional<Value> getParameter(IRI property) {
    List<Value> values = parameters.get(property);
    if (values == null || values.size() > 1) {
      return Optional.empty();
    }
    return Optional.of(values.get(0));
  }

  void addParameter(IRI property, Value value) {
    parameters.computeIfAbsent(property, k -> new ArrayList<>()).add(value);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof Constraint && id.equals(((Constraint) o).id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return type + " on property " + property + " with parameters " + parameters;
  }

  private static final IRI[] PARAMETERS = new IRI[]{
          null, // id
          null, // property
          null, // type
          QueriesForConstraintCorrectionsBuilder.REGEX_PARAMETER,
          null,
          null,
          QueriesForConstraintCorrectionsBuilder.ITEM_PARAMETER,
          QueriesForConstraintCorrectionsBuilder.PROPERTY_PARAMETER,
          null,
          QueriesForConstraintCorrectionsBuilder.CLASS_PARAMETER,
          QueriesForConstraintCorrectionsBuilder.RELATION_PARAMETER
  };

  public static Map<IRI, Constraint> read(Path file, ValueFactory valueFactory) throws IOException {
    return Files.newBufferedReader(file, StandardCharsets.UTF_8).lines().skip(1).map(l -> {
      String[] parts = l.split("\t");
      Map<IRI, List<Value>> parameters = new HashMap<>();
      for (int i = 0; i < PARAMETERS.length; i++) {
        if (PARAMETERS[i] != null && parts.length > i) {
          String value = parts[i].trim();
          if (!value.isEmpty()) {
            List<Value> values = value.startsWith("\"")
                    ? Collections.singletonList(NTriplesUtil.parseValue(value, valueFactory))
                    : Arrays.stream(value.split(" ")).map(v -> NTriplesUtil.parseValue(v, valueFactory)).collect(Collectors.toList());
            parameters.put(PARAMETERS[i], values);
          }
        }
      }
      return new Constraint(NTriplesUtil.parseURI(parts[0], valueFactory), NTriplesUtil.parseURI(parts[1], valueFactory), NTriplesUtil.parseURI(parts[2], valueFactory), parameters);
    }).collect(Collectors.toMap(Constraint::getId, c -> c));
  }
}
