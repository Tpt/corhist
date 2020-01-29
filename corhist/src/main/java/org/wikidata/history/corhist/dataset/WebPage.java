package org.wikidata.history.corhist.dataset;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class WebPage implements ContextElement {
  private final URI location;
  private final int statusCode;
  private final String content;

  @JsonCreator
  public WebPage(@JsonProperty("location") URI location, @JsonProperty("statusCode") int statusCode, @JsonProperty("content") String content) {
    this.location = location;
    this.statusCode = statusCode;
    this.content = content;
  }

  public String getType() {
    return "page";
  }

  public URI getLocation() {
    return location;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public String getContent() {
    return content;
  }
}

