package org.wikidata.history.corhist.mining;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
import org.wikidata.history.corhist.dataset.ConstraintViolationCorrectionWithContext;
import org.wikidata.history.corhist.mining.ConstraintRuleWithContext.ContextualBinding;
import org.wikidata.history.corhist.mining.ConstraintRuleWithContext.SimplePattern;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class MinerFromContext {

  private static final int MIN_SUPPORT = 10;
  static final float MIN_STD_CONFIDENCE = 0.1f;
  private static final float IMPROVEMENT_STEP = 0.05f;
  private static final int MAX_SIZE = 3;

  private static final Var S = new Var("s");
  private static final Var O = new Var("o");
  private static final Var OTHER_S = new Var("otherS");
  private static final Var OTHER_O = new Var("otherO");

  List<ConstraintRuleWithContext> mine(List<ConstraintViolationCorrectionWithContext> corrections) {
    return possibleBasicRules(corrections)
            .flatMap(rule -> refineWithGraph(rule, 1))
            .distinct()
            .collect(Collectors.toList());
  }

  private Stream<ConstraintRuleWithContext> possibleBasicRules(List<ConstraintViolationCorrectionWithContext> corrections) {
    Map<Map.Entry<SimplePattern, Optional<SimplePattern>>, List<ContextualBinding>> violationPatterns = new HashMap<>();
    Map<Map.Entry<Map.Entry<SimplePattern, Optional<SimplePattern>>, Set<StatementPattern>>, List<ContextualBinding>> violationAndCorrectionPatterns = new HashMap<>();

    corrections.forEach(correction -> {
      //We first build a pattern for the violation and we expand it for the correction
      createViolationPattern(correction).forEach(violationPatternAndBinding -> {
        Map.Entry<SimplePattern, Optional<SimplePattern>> violationPattern = violationPatternAndBinding.getKey();
        ContextualBinding binding = violationPatternAndBinding.getValue();
        Set<StatementPattern> correctionPatterns = buildCorrectionPattern(
                binding,
                correction.getCorrection().getCorrection()
        );
        violationPatterns.computeIfAbsent(
                violationPattern,
                (k) -> new ArrayList<>()
        ).add(binding);
        violationAndCorrectionPatterns.computeIfAbsent(
                Map.entry(violationPattern, correctionPatterns),
                (k) -> new ArrayList<>()
        ).add(binding);
      });
    });

    //We transform the patterns we have found in rules
    return violationAndCorrectionPatterns.entrySet().stream().map(violationAndCorrectionPatternAndBindings -> {
      Map.Entry<SimplePattern, Optional<SimplePattern>> violationPattern = violationAndCorrectionPatternAndBindings.getKey().getKey();
      Set<StatementPattern> correctionPatterns = violationAndCorrectionPatternAndBindings.getKey().getValue();
      List<ContextualBinding> bodyBindings = violationPatterns.get(violationPattern);
      List<ContextualBinding> fullBindings = violationAndCorrectionPatternAndBindings.getValue();
      return new ConstraintRuleWithContext(correctionPatterns, violationPattern.getKey(), violationPattern.getValue().stream().collect(Collectors.toList()), bodyBindings, fullBindings);
    }).filter(this::passThresholds);
  }


  private Stream<Map.Entry<Map.Entry<SimplePattern, Optional<SimplePattern>>, ContextualBinding>> createViolationPattern(ConstraintViolationCorrectionWithContext correction) {
    Value subject = correction.getCorrection().getTargetTriple().getSubject();
    IRI predicate = correction.getCorrection().getConstraint().getId();
    Value object = correction.getCorrection().getTargetTriple().getObject();
    Statement otherTriple = correction.getCorrection().getOtherTargetTriple();

    List<Map.Entry<Map.Entry<SimplePattern, Optional<SimplePattern>>, ContextualBinding>> possibles = new ArrayList<>();
    possibles.add(Map.entry(
            Map.entry(
                    new SimplePattern("s", predicate, "o"),
                    Optional.empty()
            ),
            new ContextualBinding(correction, subject, object, null, null)
    ));
    possibles.add(Map.entry(
            Map.entry(
                    new SimplePattern("s", predicate, correction.getCorrection().getTargetTriple().getObject()),
                    Optional.empty()
            ),
            new ContextualBinding(correction, subject, null, null, null)
    ));

    if (otherTriple != null) {
      if (otherTriple.getSubject().equals(subject)) {
        if (otherTriple.getObject().equals(object)) {
          possibles.add(Map.entry(
                  Map.entry(
                          new SimplePattern("s", predicate, "o"),
                          Optional.of(new SimplePattern("s", otherTriple.getPredicate(), "o"))
                  ),
                  new ContextualBinding(correction, subject, object, null, null)
          ));
        }
        possibles.add(Map.entry(
                Map.entry(
                        new SimplePattern("s", predicate, "o"),
                        Optional.of(new SimplePattern("s", otherTriple.getPredicate(), "otherO"))
                ),
                new ContextualBinding(correction, subject, object, null, otherTriple.getObject())
        ));
        possibles.add(Map.entry(
                Map.entry(
                        new SimplePattern("s", predicate, "o"),
                        Optional.of(new SimplePattern("s", otherTriple.getPredicate(), otherTriple.getObject()))
                ),
                new ContextualBinding(correction, subject, object, null, null)
        ));
        possibles.add(Map.entry(
                Map.entry(
                        new SimplePattern("s", predicate, object),
                        Optional.of(new SimplePattern("s", otherTriple.getPredicate(), otherTriple.getObject()))
                ),
                new ContextualBinding(correction, subject, null, null, null)
        ));
        possibles.add(Map.entry(
                Map.entry(
                        new SimplePattern("s", predicate, object),
                        Optional.of(new SimplePattern("s", otherTriple.getPredicate(), "otherO"))
                ),
                new ContextualBinding(correction, subject, null, null, otherTriple.getObject())
        ));
      } else if (otherTriple.getSubject().equals(object)) {
        if (otherTriple.getObject().equals(subject)) {
          possibles.add(Map.entry(
                  Map.entry(
                          new SimplePattern("s", predicate, "o"),
                          Optional.of(new SimplePattern("o", otherTriple.getPredicate(), "s"))
                  ),
                  new ContextualBinding(correction, subject, object, null, null)
          ));
        }
        possibles.add(Map.entry(
                Map.entry(
                        new SimplePattern("s", predicate, "o"),
                        Optional.of(new SimplePattern("o", otherTriple.getPredicate(), "otherO"))
                ),
                new ContextualBinding(correction, subject, object, null, otherTriple.getObject())
        ));
        possibles.add(Map.entry(
                Map.entry(
                        new SimplePattern("s", predicate, "o"),
                        Optional.of(new SimplePattern("o", otherTriple.getPredicate(), otherTriple.getObject()))
                ),
                new ContextualBinding(correction, subject, object, null, null)
        ));
      }
      if (otherTriple.getObject().equals(object)) {
        possibles.add(Map.entry(
                Map.entry(
                        new SimplePattern("s", predicate, "o"),
                        Optional.of(new SimplePattern("otherS", otherTriple.getPredicate(), "o"))
                ),
                new ContextualBinding(correction, subject, object, otherTriple.getSubject(), null)
        ));
      } else if (otherTriple.getObject().equals(subject)) {
        possibles.add(Map.entry(
                Map.entry(
                        new SimplePattern("s", predicate, "o"),
                        Optional.of(new SimplePattern("otherS", otherTriple.getPredicate(), "s"))
                ),
                new ContextualBinding(correction, subject, object, otherTriple.getSubject(), null)
        ));
      }
    }
    return possibles.stream();
  }

  private Set<StatementPattern> buildCorrectionPattern(ContextualBinding binding, Set<Statement> statements) {
    return statements.stream().map(statement -> {
      Var subject = buildCorrectionVariable(statement.getSubject(), binding);
      Var predicate = TupleExprs.createConstVar(statement.getPredicate());
      Var object = buildCorrectionVariable(statement.getObject(), binding);
      Var context = TupleExprs.createConstVar(statement.getContext());
      return new StatementPattern(subject, predicate, object, context);
    }).collect(Collectors.toSet());
  }

  private Var buildCorrectionVariable(Value value, ContextualBinding binding) {
    if (value.equals(binding.getValue("s"))) {
      return S;
    } else if (value.equals(binding.getValue("o"))) {
      return O;
    } else if (value.equals(binding.getValue("otherS"))) {
      return OTHER_S;
    } else if (value.equals(binding.getValue("otherO"))) {
      return OTHER_O;
    } else {
      return TupleExprs.createConstVar(value);
    }
  }

  private boolean passThresholds(ConstraintRuleWithContext rule) {
    return rule.getSupport() >= MIN_SUPPORT && rule.getStdConfidence() >= MIN_STD_CONFIDENCE;
  }

  private Stream<ConstraintRuleWithContext> refineWithGraph(ConstraintRuleWithContext rule, int depth) {
    Set<String> variables = getVariables(rule);
    return Stream.concat(
            variables.stream().flatMap(var -> {
              Stats stats = statsForVariable(var, rule);
              return Stream.concat(
                      //TODO: set object
                      stats.possiblePredicates()
                              .flatMap(p -> Stream.concat(
                                      Stream.of(new SimplePattern(var, p, (Value) null)),
                                      variables.stream().map(v -> new SimplePattern(var, p, v))
                              ))
                              .filter(pattern -> !rule.getContextBody().contains(pattern)),
                      stats.possiblePredicateObjects()
                              .map(po -> new SimplePattern(var, po.getKey(), po.getValue()))
                              .filter(pattern -> !rule.getContextBody().contains(pattern))
                              .filter(pattern -> !rule.getContextBody().contains(new SimplePattern(pattern.subjectVariable, pattern.predicate, (Value) null)))
              )
                      .map(pattern -> new ConstraintRuleWithContext(
                              rule.getHead(), rule.getViolationBody(), concat(rule.getContextBody(), pattern),
                              rule.getBodyBindings()
                                      .filter(binding -> binding.matches(pattern))
                                      .collect(Collectors.toList()),
                              rule.getFullBindings()
                                      .filter(binding -> binding.matches(pattern))
                                      .collect(Collectors.toList())
                      ))
                      .filter(newRule -> passThresholds(newRule) && newRule.getStdConfidence() > rule.getStdConfidence() + IMPROVEMENT_STEP)
                      .flatMap(newRule -> {
                        if (depth < MAX_SIZE) {
                          return refineWithGraph(newRule, depth + 1);
                        } else {
                          return Stream.of(newRule);
                        }
                      });
            }),
            Stream.of(rule)
    );
  }

  private Stats statsForVariable(String var, ConstraintRuleWithContext rule) {
    Stats stats = new Stats();
    rule.getFullBindings().forEach(binding -> stats.add(binding.correction.getPsoContext(), binding.getValue(var)));
    return stats;
  }

  private Set<String> getVariables(ConstraintRuleWithContext rule) {
    return Stream.concat(
            Stream.concat(rule.getHead().stream().flatMap(this::getVariables), getVariables(rule.getViolationBody())),
            rule.getContextBody().stream().flatMap(this::getVariables)
    ).collect(Collectors.toSet());
  }

  private Stream<String> getVariables(SimplePattern pattern) {
    return (pattern.objectVariable == null)
            ? Stream.of(pattern.subjectVariable)
            : Stream.of(pattern.subjectVariable, pattern.objectVariable);
  }

  private Stream<String> getVariables(StatementPattern pattern) {
    Stream.Builder<String> builder = Stream.builder();
    if (!pattern.getSubjectVar().isConstant()) {
      builder.accept(pattern.getSubjectVar().getName());
    }
    if (!pattern.getPredicateVar().isConstant()) {
      builder.accept(pattern.getPredicateVar().getName());
    }
    if (!pattern.getObjectVar().isConstant()) {
      builder.accept(pattern.getObjectVar().getName());
    }
    return builder.build();
  }

  private <T> List<T> concat(List<T> a, T b) {
    List<T> result = new ArrayList<>(a);
    result.add(b);
    return result;
  }

  private static class Stats {
    private final Map<IRI, Set<Value>> predicateCount = new HashMap<>();
    private final Map<Map.Entry<IRI, Value>, Set<Value>> predicateObjectCount = new HashMap<>();

    void add(Map<IRI, List<Map.Entry<Value, Value>>> model, Value value) {
      model.forEach((p, sos) -> sos.forEach(so -> {
        if (so.getKey().equals(value)) {
          predicateCount.computeIfAbsent(p, i -> new HashSet<>()).add(so.getKey());
          predicateObjectCount.computeIfAbsent(Map.entry(p, so.getValue()), i -> new HashSet<>()).add(so.getKey());
        }
      }));
    }

    private Stream<IRI> possiblePredicates() {
      return predicateCount.entrySet().stream().filter(e -> e.getValue().size() >= MIN_SUPPORT).map(Map.Entry::getKey);
    }

    private Stream<Map.Entry<IRI, Value>> possiblePredicateObjects() {
      return predicateObjectCount.entrySet().stream().filter(e -> e.getValue().size() >= MIN_SUPPORT).map(Map.Entry::getKey);
    }
  }
}
