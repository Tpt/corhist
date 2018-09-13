package org.wikidata.history.mining;

final class Evaluation {

  private float precision;
  private float recall;
  private int testSetSize;

  Evaluation(float precision, float recall, int testSetSize) {
    this.precision = precision;
    this.recall = recall;
    this.testSetSize = testSetSize;
  }

  public float getPrecision() {
    return precision;
  }

  public float getRecall() {
    return recall;
  }

  public float getF1() {
    return (2 * precision * recall) / (precision + recall);
  }

  public int getTestSetSize() {
    return testSetSize;
  }
}
