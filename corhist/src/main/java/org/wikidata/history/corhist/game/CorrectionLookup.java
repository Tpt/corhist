package org.wikidata.history.corhist.game;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.corhist.mining.PatternEvaluator;
import org.wikidata.history.corhist.mining.SimpleConstraintRule;
import org.wikidata.history.sparql.Vocabulary;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class CorrectionLookup {

  private static final Logger LOGGER = LoggerFactory.getLogger(CorrectionLookup.class);
  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  private final List<SimpleConstraintRule> rules = new ArrayList<>();

  public CorrectionLookup(String filesToLoad) {
    this.loadRules(filesToLoad);
  }

  private void loadRules(String filesToLoad) {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get("./"), filesToLoad)) {
      stream.forEach(path -> {
        int count = 0;
        try (ObjectInputStream inputStream = new ObjectInputStream(Files.newInputStream(path))) {
          while (true) {
            rules.add(removeContext((SimpleConstraintRule) inputStream.readObject()));
            count++;
          }
        } catch (EOFException e) {
          //It's the end, everything is ok
        } catch (IOException | ClassNotFoundException e) {
          LOGGER.error(e.getMessage(), e);
        }
        LOGGER.info(count + " rules loaded from " + path);
      });
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  private SimpleConstraintRule removeContext(SimpleConstraintRule rule) {
    return new SimpleConstraintRule(
            new HashSet<>(rule.getHead()),
            removeContext(rule.getViolationBody()),
            rule.getContextBody().stream().map(this::removeContext).collect(Collectors.toList())
    );
  }

  private StatementPattern removeContext(StatementPattern pattern) {
    return new StatementPattern(pattern.getScope(), pattern.getSubjectVar(), pattern.getPredicateVar(), pattern.getObjectVar());
  }

  Optional<Set<Statement>> buildPossibleCorrection(IRI constraint, Resource subject, IRI property, IRI statement, Model model) {
    return getObjects(statement, VALUE_FACTORY.createIRI(Vocabulary.PS_NAMESPACE, property.getLocalName()), model)
            .flatMap(object -> rules.stream().flatMap(rule ->
                            PatternEvaluator.evaluate(
                                    rule.getViolationBody(),
                                    VALUE_FACTORY.createStatement(subject, constraint, object)
                            ).flatMap(violationBindings ->
                                    PatternEvaluator.evaluate(rule.getContextBody(), violationBindings, model)
                                            .map(bindings -> PatternEvaluator.instantiate(rule.getHead(), bindings, VALUE_FACTORY))
                            )
                    )
            ).findAny();
  }

  private Stream<Value> getObjects(Resource subject, IRI predicate, Model model) {
    return model.filter(subject, predicate, null).objects().stream().distinct();
  }

  public static void main(String[] args) {
    ValueFactory valueFactory = SimpleValueFactory.getInstance();
    CorrectionLookup correctionLookup = new CorrectionLookup("*.ser");

    IRI constraint = valueFactory.createIRI(Vocabulary.WDS_NAMESPACE, "P21-09D3E4D3-BBC5-4F40-9BB7-CC96C7721A56");
    IRI entity = valueFactory.createIRI(Vocabulary.WD_NAMESPACE, "Q42");
    IRI property = valueFactory.createIRI(Vocabulary.WDT_NAMESPACE, "P21");
    IRI statement = valueFactory.createIRI(Vocabulary.WDS_NAMESPACE, "Q42-error");
    IRI wrongValue = valueFactory.createIRI(Vocabulary.WD_NAMESPACE, "Q360210");

    Model model = new LinkedHashModel();
    model.add(entity, valueFactory.createIRI(Vocabulary.P_NAMESPACE, property.getLocalName()), statement);
    model.add(statement, valueFactory.createIRI(Vocabulary.PS_NAMESPACE, property.getLocalName()), wrongValue);
    model.add(entity, valueFactory.createIRI(Vocabulary.WDT_NAMESPACE, property.getLocalName()), wrongValue);

    System.out.println(correctionLookup.buildPossibleCorrection(constraint, entity, property, statement, model));
  }
}
