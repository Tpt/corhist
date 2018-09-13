package org.wikidata.history.sparql;

class NotSupportedValueException extends Exception {
  NotSupportedValueException() {
    super("Not supported value");
  }
}
