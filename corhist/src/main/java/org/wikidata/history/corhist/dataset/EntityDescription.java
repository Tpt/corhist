package org.wikidata.history.corhist.dataset;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class EntityDescription implements ContextElement {
  private final IRI id;
  private final Map<IRI, List<Value>> facts;
  private final Map<String, String> labels;
  private final Map<String, String> descriptions;

  public EntityDescription(IRI id) {
    this.id = id;
    this.facts = new HashMap<>();
    this.labels = new HashMap<>();
    this.descriptions = new HashMap<>();
  }

  @JsonCreator
  EntityDescription(
          @JsonProperty("id") String id,
          @JsonProperty("facts") Map<String, List<String>> facts,
          @JsonProperty("labels") Map<String, String> labels,
          @JsonProperty("descriptions") Map<String, String> descriptions
  ) {
    this.id = NTriplesUtil.parseURI(id, SimpleValueFactory.getInstance());
    this.facts = facts.entrySet().stream().collect(Collectors.toMap(
            e -> NTriplesUtil.parseURI(e.getKey(), SimpleValueFactory.getInstance()),
            e -> e.getValue().stream().map(v -> NTriplesUtil.parseValue(v, SimpleValueFactory.getInstance())).collect(Collectors.toList())
    ));
    this.labels = labels;
    this.descriptions = descriptions;
  }

  public void addFact(IRI predicate, Value object) {
    facts.computeIfAbsent(predicate, e -> new ArrayList<>()).add(object);
  }

  public void setLabel(Value label) {
    labels.put(((Literal) label).getLanguage().get(), label.stringValue());
  }

  public void setDescription(Value description) {
    descriptions.put(((Literal) description).getLanguage().get(), description.stringValue());
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
