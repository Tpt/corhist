package org.wikidata.history.corhist;

import org.eclipse.rdf4j.model.*;

public class ConvertingValueFactory {

  private final ValueFactory valueFactory;

  public ConvertingValueFactory(ValueFactory valueFactory) {
    this.valueFactory = valueFactory;
  }

  public IRI convert(IRI iri) {
    return valueFactory.createIRI(iri.toString());
  }

  public BNode convert(BNode bnode) {
    return valueFactory.createBNode(bnode.getID());
  }

  public Literal convert(Literal literal) {
    return literal.getLanguage()
            .map(language -> valueFactory.createLiteral(literal.getLabel(), language))
            .orElseGet(() -> valueFactory.createLiteral(literal.getLabel(), literal.getDatatype()));
  }

  public Resource convert(Resource resource) {
    if (resource instanceof IRI) {
      return convert((IRI) resource);
    } else if (resource instanceof BNode) {
      return convert((BNode) resource);
    } else {
      throw new IllegalArgumentException(resource + " is not a known resource");
    }
  }

  public Value convert(Value value) {
    if (value instanceof IRI) {
      return convert((IRI) value);
    } else if (value instanceof BNode) {
      return convert((BNode) value);
    } else if (value instanceof Literal) {
      return convert((Literal) value);
    } else {
      throw new IllegalArgumentException(value + " is not a known value");
    }
  }
}
