package org.wikidata.history.corhist.mining;

final class Evaluation {

  private float precision;
  private float recall;
  private int testSetSize;

  Evaluation(float precision, float recall, int testSetSize) {
    this.precision = precision;
    this.recall = recall;
    this.testSetSize = testSetSize;
  }

  float getPrecision() {
    return precision;
  }

  float getRecall() {
    return recall;
  }

  float getF1() {
    return (2 * precision * recall) / (precision + recall);
  }

  int getTestSetSize() {
    return testSetSize;
  }
}
