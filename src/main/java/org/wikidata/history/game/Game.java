package org.wikidata.history.game;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface Game {

  Description getDescription();

  List<Tile> generateTiles(int count, String language);

  void log(String user, int tile, String decision);


  /**
   * TODO: add option support to
   * options: [{"name": "Entry Type", "key": "type", "values": { "any": "Any", "person": Person"}}]
   **/
  final class Description {
    private final Map<String, String> labels = new HashMap<>();
    private final Map<String, String> descriptions = new HashMap<>();

    @JsonProperty("label")
    Map<String, String> getLabels() {
      return labels;
    }

    void setLabel(String language, String text) {
      labels.put(language, text);
    }

    @JsonProperty("description")
    Map<String, String> getDescriptions() {
      return descriptions;
    }

    void setDescription(String language, String text) {
      descriptions.put(language, text);
    }
  }

  final class Tile {
    private final int id;
    private final List<Section> sections = new ArrayList<>();
    private final List<Button> controls = new ArrayList<>();

    Tile(int id) {
      this.id = id;
    }

    @JsonProperty("id")
    int getId() {
      return id;
    }

    @JsonProperty("sections")
    List<Section> getSections() {
      return sections;
    }

    void addSection(Section section) {
      sections.add(section);
    }

    @JsonProperty("controls")
    List<Button> getControls() {
      return controls;
    }

    void addControl(Button control) {
      controls.add(control);
    }
  }

  interface Section {
  }

  final class ItemSection implements Section {
    private final String id;

    ItemSection(String id) {
      this.id = id;
    }

    @JsonProperty("q")
    String getId() {
      return id;
    }
  }

  final class TextSection implements Section {
    private String title;
    private String text;

    TextSection(String title, String text) {
      this.title = title;
      this.text = text;
    }

    @JsonProperty("title")
    String getTitle() {
      return title;
    }

    @JsonProperty("text")
    String getText() {
      return text;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  final class Button {
    private final String type;
    private final String decision;
    private final String label;
    private final Map<String, String> action;

    Button(String type, String decision, String label) {
      this(type, decision, label, null);
    }

    Button(String type, String decision, String label, Map<String, String> action) {
      this.type = type;
      this.decision = decision;
      this.label = label;
      this.action = action;
    }

    @JsonProperty("type")
    String getType() {
      return type;
    }

    @JsonProperty("decision")
    String getDecision() {
      return decision;
    }

    @JsonProperty("label")
    String getLabel() {
      return label;
    }

    @JsonProperty("api_action")
    Map<String, String> getAction() {
      return action;
    }
  }
}
