package org.wikidata.history.game;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
import org.wikidata.history.mining.SimpleConstraintRule;
import org.wikidata.history.sparql.Vocabulary;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

public class DemoData {
  public static void main(String[] args) throws IOException, ClassNotFoundException {
    ValueFactory valueFactory = SimpleValueFactory.getInstance();
    try (ObjectOutputStream outputStream = new ObjectOutputStream(Files.newOutputStream(Paths.get("constraint-rules-demo.ser")))) {
      outputStream.writeObject(new SimpleConstraintRule(
              Collections.singleton(new StatementPattern(
                      new Var("s"),
                      TupleExprs.createConstVar(valueFactory.createIRI(Vocabulary.WDT_NAMESPACE, "P361")),
                      new Var("o"),
                      TupleExprs.createConstVar(Vocabulary.HISTORY_DELETION)
              )),
              new StatementPattern(new Var("s"), new Var("c"), new Var("o")),
              Collections.emptyList()
      ));
    }
    try (ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream("constraint-rules-demo.ser"))) {
      System.out.println(inputStream.readObject());
    }
  }
}
