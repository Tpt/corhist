package org.wikidata.history.corhist.game;

import java.util.Map;

class PossibleCorrection {
  private final int id;
  private final String entityId;
  private final String propertyId;
  private final String statementId;
  private final String constraintId;
  private final String constraintType;
  private final String message;
  private final Map<String, String> edit;

  PossibleCorrection(int id, String entityId, String propertyId, String statementId, String constraintId, String constraintType, String message, Map<String, String> edit) {
    this.id = id;
    this.entityId = entityId;
    this.propertyId = propertyId;
    this.statementId = statementId;
    this.constraintId = constraintId;
    this.constraintType = constraintType;
    this.message = message;
    this.edit = edit;
  }

  public int getId() {
    return id;
  }

  public String getEntityId() {
    return entityId;
  }

  public String getMessage() {
    return message;
  }

  public Map<String, String> getEdit() {
    return edit;
  }
}