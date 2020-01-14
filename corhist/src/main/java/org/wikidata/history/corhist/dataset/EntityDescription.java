package org.wikidata.history.corhist.dataset;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class EntityDescription implements ContextElement {
  private final IRI id;
  private final Map<IRI, List<Value>> facts = new HashMap<>();
  private final Map<String, String> labels = new HashMap<>();
  private final Map<String, String> descriptions = new HashMap<>();

  public EntityDescription(IRI id) {
    this.id = id;
  }

  public void addFact(IRI predicate, Value object) {
    facts.computeIfAbsent(predicate, e -> new ArrayList<>()).add(object);
  }

  public void setLabel(Value label) {
    labels.put(((Literal) label).getLanguage().get(), label.stringValue());
  }

  public void setDescription(Value description) {
    labels.put(((Literal) description).getLanguage().get(), description.stringValue());
  }

  public String getType() {
    return "entity";
  }

  @JsonIgnore
  public IRI getId() {
    return id;
  }

  @JsonProperty("id")
  public String getStringId() {
    return NTriplesUtil.toNTriplesString(id);
  }

  @JsonIgnore
  public Map<IRI, List<Value>> getFacts() {
    return facts;
  }

  @JsonProperty("facts")
  public Map<String, List<String>> getStringFacts() {
    return facts.entrySet().stream().collect(Collectors.toMap(
            e -> NTriplesUtil.toNTriplesString(e.getKey()),
            e -> e.getValue().stream().map(NTriplesUtil::toNTriplesString).collect(Collectors.toList())
    ));
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public Map<String, String> getDescriptions() {
    return descriptions;
  }
}
