package org.wikidata.history.mining;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.wikidata.history.dataset.ConstraintViolationCorrection;
import org.wikidata.history.sparql.Vocabulary;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class Miner {

  private static final int MIN_SUPPORT = 10;
  static final float MIN_STD_CONFIDENCE = 0.5f;
  private static final float IMPROVEMENT_STEP = 0.05f;

  private static final Var S = new Var("s");
  private static final Var P = new Var("p");
  private static final Var O = new Var("o");
  private static final Var V = new Var("v");
  private static final Var G = new Var("g");

  private static final List<String> SG = Arrays.asList("s", "g");
  private static final List<String> SOG = Arrays.asList("s", "o", "g");

  private final RepositoryConnection repositoryConnection;

  Miner(RepositoryConnection repositoryConnection) {
    this.repositoryConnection = repositoryConnection;
  }

  List<ConstraintRule> mine(List<ConstraintViolationCorrection> corrections) {
    return possibleBasicRules(corrections)
            .flatMap(this::refineWithGraph)
            .distinct()
            .collect(Collectors.toList());
  }

  private Stream<ConstraintRule> possibleBasicRules(List<ConstraintViolationCorrection> corrections) {
    Map<StatementPattern, List<BindingSet>> violationPatterns = new HashMap<>();
    Map<Pair<StatementPattern, Set<StatementPattern>>, List<BindingSet>> violationAndCorrectionPatterns = new HashMap<>();

    corrections.forEach(correction -> {
      //We first build a pattern for the violation and we expand it for the correction
      createViolationPattern(correction).forEach(violationPatternAndBinding -> {
        StatementPattern violationPattern = violationPatternAndBinding.getLeft();
        BindingSet bindingSet = violationPatternAndBinding.getRight();
        Set<StatementPattern> correctionPatterns = buildCorrectionPattern(
                violationPatternAndBinding.getRight(),
                correction.getCorrection()
        );
        violationPatterns.computeIfAbsent(
                violationPattern,
                (k) -> new ArrayList<>()
        ).add(bindingSet);
        violationAndCorrectionPatterns.computeIfAbsent(
                Pair.of(violationPattern, correctionPatterns),
                (k) -> new ArrayList<>()
        ).add(bindingSet);
      });
    });

    //We transform the patterns we have found in rules
    return violationAndCorrectionPatterns.entrySet().stream().map(violationAndCorrectionPatternAndBindings -> {
      StatementPattern violationPattern = violationAndCorrectionPatternAndBindings.getKey().getLeft();
      Set<StatementPattern> correctionPatterns = violationAndCorrectionPatternAndBindings.getKey().getRight();
      List<BindingSet> bodyBindings = violationPatterns.get(violationPattern);
      List<BindingSet> fullBindings = violationAndCorrectionPatternAndBindings.getValue();
      return new ConstraintRule(correctionPatterns, violationPattern, bodyBindings, fullBindings);
    }).filter(this::passThresholds);
  }


  private Stream<Pair<StatementPattern, BindingSet>> createViolationPattern(ConstraintViolationCorrection correction) {
    Value subject = correction.getTargetTriple().getSubject();
    Var predicate = TupleExprs.createConstVar(correction.getConstraint().getId());
    Value object = correction.getTargetTriple().getObject();
    Value graph = Vocabulary.toGlobalState(Vocabulary.previousRevision(correction.getCorrectionRevision())); //TODO: before first correction part
    return Stream.of(
            Pair.of(
                    new StatementPattern(S, predicate, O, G),
                    new ListBindingSet(SOG, subject, object, graph)
            ),
            Pair.of(
                    new StatementPattern(S, predicate, TupleExprs.createConstVar(correction.getTargetTriple().getObject()), G),
                    new ListBindingSet(SG, subject, graph)
            )
    );
  }

  private Set<StatementPattern> buildCorrectionPattern(BindingSet bindingSet, Set<Statement> statements) {
    return statements.stream().map(statement -> {
      //TODO: keep a field constant even if it matches a variable?
      Var subject = buildBasicVarForValue(statement.getSubject(), bindingSet);
      Var predicate = TupleExprs.createConstVar(statement.getPredicate());
      Var object = buildBasicVarForValue(statement.getObject(), bindingSet);
      Var context = TupleExprs.createConstVar(statement.getContext());
      return new StatementPattern(subject, predicate, object, context);
    }).collect(Collectors.toSet());
  }

  private Var buildBasicVarForValue(Value value, BindingSet fromBindingSet) {
    for (String name : fromBindingSet.getBindingNames()) {
      if (value.equals(fromBindingSet.getValue(name))) {
        return new Var(name);
      }
    }
    return TupleExprs.createConstVar(value);
  }

  private boolean passThresholds(ConstraintRule rule) {
    return rule.getSupport() >= MIN_SUPPORT && rule.getStdConfidence() >= MIN_STD_CONFIDENCE;
  }

  private Stream<ConstraintRule> refineWithGraph(ConstraintRule rule) {
    return Stream.concat(
            getVariables(rule).flatMap(var -> {
              if (var.equals(G)) {
                return Stream.empty();
              }
              return rule.getFullBindings()
                      .flatMap(bindings -> bindings.hasBinding(var.getName()) ? Stream.of(bindings.getValue(var.getName())) : Stream.empty())
                      .distinct() //TODO: useful
                      .flatMap(value ->
                              PatternEvaluator.evaluate(value, null, null, null, repositoryConnection).flatMap(statement -> Stream.of(
                                      new StatementPattern(var, TupleExprs.createConstVar(statement.getPredicate()), TupleExprs.createConstVar(statement.getObject()), G),
                                      new StatementPattern(var, TupleExprs.createConstVar(statement.getPredicate()), V, G) //TODO: create var
                              ))
                      )
                      .distinct()
                      .map(pattern -> ruleWithAdditionalHistoryBody(rule, pattern))
                      .filter(newRule -> passThresholds(newRule) && newRule.getStdConfidence() > rule.getStdConfidence() + IMPROVEMENT_STEP);
            }),
            Stream.of(rule)
    );
  }

  private Stream<Var> getVariables(ConstraintRule rule) {
    return Stream.concat(
            Stream.concat(getVariables(rule.getHead()), getVariables(rule.getViolationBody())),
            getVariables(rule.getContextBody())
    ).distinct();
  }

  private Stream<Var> getVariables(Collection<StatementPattern> patterns) {
    return patterns.stream().flatMap(this::getVariables);
  }

  private Stream<Var> getVariables(StatementPattern pattern) {
    Stream.Builder<Var> builder = Stream.builder();
    if (!pattern.getSubjectVar().isConstant()) {
      builder.accept(pattern.getSubjectVar());
    }
    if (!pattern.getPredicateVar().isConstant()) {
      builder.accept(pattern.getPredicateVar());
    }
    if (!pattern.getObjectVar().isConstant()) {
      builder.accept(pattern.getObjectVar());
    }
    if (pattern.getContextVar() != null && !pattern.getContextVar().isConstant()) {
      builder.accept(pattern.getContextVar());
    }
    return builder.build();
  }

  private ConstraintRule ruleWithAdditionalHistoryBody(ConstraintRule rule, StatementPattern pattern) {
    return new ConstraintRule(
            rule.getHead(), rule.getViolationBody(), concat(rule.getContextBody(), pattern),
            rule.getBodyBindings().flatMap(bindings -> PatternEvaluator.evaluate(pattern, bindings, repositoryConnection)).collect(Collectors.toList()),
            rule.getFullBindings().flatMap(bindings -> PatternEvaluator.evaluate(pattern, bindings, repositoryConnection)).collect(Collectors.toList())
    );
  }

  private <T> List<T> concat(List<T> a, T b) {
    List<T> result = new ArrayList<>(a);
    result.add(b);
    return result;
  }

}
