package org.wikidata.history.corhist.dataset;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.net.URI;

public final class WebPage implements ContextElement {
  private final URI location;
  private final int statusCode;
  private final String content;

  @JsonCreator
  public WebPage(URI location, int statusCode, String content) {
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

