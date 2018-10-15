package org.wikidata.history.dataset;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class Constraint {

  private static final Logger LOGGER = LoggerFactory.getLogger(Constraint.class);

  private IRI id;
  private IRI property;
  private IRI type;
  private Map<IRI, List<Value>> parameters;

  Constraint(IRI id, IRI property, IRI type, Map<IRI, List<Value>> parameters) {
    this.id = id;
    this.property = property;
    this.type = type;
    this.parameters = parameters;
  }

  Constraint(IRI id, IRI property, IRI type) {
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
}
