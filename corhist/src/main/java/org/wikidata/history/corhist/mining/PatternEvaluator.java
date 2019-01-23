package org.wikidata.history.corhist.mining;

import org.eclipse.rdf4j.common.iterator.CloseableIterationIterator;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;

import java.util.Collection;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class PatternEvaluator {

  private static final BindingSet EMPTY_BINDING_SET = new EmptyBindingSet();

  public static Stream<BindingSet> evaluate(StatementPattern pattern, Statement statement) {
    return evaluate(pattern, statement, EMPTY_BINDING_SET);
  }

  private static Stream<BindingSet> evaluate(StatementPattern pattern, Statement statement, BindingSet initialBindingSet) {
    QueryBindingSet bindingSet = new QueryBindingSet(initialBindingSet);
    bindingSet = addToBindingSet(pattern.getSubjectVar(), statement.getSubject(), bindingSet);
    bindingSet = addToBindingSet(pattern.getPredicateVar(), statement.getPredicate(), bindingSet);
    bindingSet = addToBindingSet(pattern.getObjectVar(), statement.getObject(), bindingSet);
    bindingSet = addToBindingSet(pattern.getContextVar(), statement.getContext(), bindingSet);
    return bindingSet == null ? Stream.empty() : Stream.of(bindingSet);
  }

  private static QueryBindingSet addToBindingSet(Var var, Value value, QueryBindingSet bindingSet) {
    if (var == null || bindingSet == null) {
      return bindingSet;
    } else if (value == null) {
      return null;
    } else if (var.isConstant()) {
      return value.equals(var.getValue()) ? bindingSet : null;
    } else if (bindingSet.hasBinding(var.getName())) {
      return value.equals(bindingSet.getValue(var.getName())) ? bindingSet : null;
    } else {
      bindingSet.addBinding(var.getName(), value);
      return bindingSet;
    }
  }

  static Stream<BindingSet> evaluate(StatementPattern pattern, BindingSet bindingSet, RepositoryConnection connection) {
    return evaluate(
            getValue(pattern.getSubjectVar(), bindingSet),
            getValue(pattern.getPredicateVar(), bindingSet),
            getValue(pattern.getObjectVar(), bindingSet),
            pattern.getContextVar() == null ? null : getValue(pattern.getContextVar(), bindingSet),
            connection
    ).flatMap(statement -> evaluate(pattern, statement, bindingSet));
  }

  static Stream<Statement> evaluate(Value subject, Value predicate, Value object, Value context, RepositoryConnection connection) {
    try {
      if (subject != null && !(subject instanceof Resource)) {
        return Stream.empty();
      }
      if (predicate != null && !(predicate instanceof IRI)) {
        return Stream.empty();
      }
      if (context != null && !(context instanceof Resource)) {
        return Stream.empty();
      }

      RepositoryResult<Statement> statements = (context == null)
              ? connection.getStatements((Resource) subject, (IRI) predicate, object)
              : connection.getStatements((Resource) subject, (IRI) predicate, object, (Resource) context);

      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
              new CloseableIterationIterator<>(statements), 0
      ), false);
    } catch (QueryEvaluationException e) {
      return Stream.empty();
    }
  }

  public static Stream<BindingSet> evaluate(Collection<StatementPattern> patterns, BindingSet bindingSet, RepositoryConnection connection) {
    return patterns.stream().reduce(
            Stream.of(bindingSet),
            (bindingStream, pattern) -> bindingStream.flatMap(binding -> evaluate(pattern, binding, connection)),
            (a, b) -> {
              throw new IllegalArgumentException("This stream should not be parallel");
            }
    );
  }

  static Stream<BindingSet> evaluate(StatementPattern pattern, BindingSet bindingSet, Model model) {
    return evaluate(
            getValue(pattern.getSubjectVar(), bindingSet),
            getValue(pattern.getPredicateVar(), bindingSet),
            getValue(pattern.getObjectVar(), bindingSet),
            pattern.getContextVar() == null ? null : getValue(pattern.getContextVar(), bindingSet),
            model
    ).flatMap(statement -> evaluate(pattern, statement, bindingSet));
  }

  static Stream<Statement> evaluate(Value subject, Value predicate, Value object, Value context, Model model) {
    try {
      if (subject != null && !(subject instanceof Resource)) {
        return Stream.empty();
      }
      if (predicate != null && !(predicate instanceof IRI)) {
        return Stream.empty();
      }
      if (context != null && !(context instanceof Resource)) {
        return Stream.empty();
      }

      Model statements = (context == null)
              ? model.filter((Resource) subject, (IRI) predicate, object)
              : model.filter((Resource) subject, (IRI) predicate, object, (Resource) context);

      return statements.stream();
    } catch (QueryEvaluationException e) {
      return Stream.empty();
    }
  }

  public static Stream<BindingSet> evaluate(Collection<StatementPattern> patterns, BindingSet bindingSet, Model model) {
    return patterns.stream().reduce(
            Stream.of(bindingSet),
            (bindingStream, pattern) -> bindingStream.flatMap(binding -> evaluate(pattern, binding, model)),
            (a, b) -> {
              throw new IllegalArgumentException("This stream should not be parallel");
            }
    );
  }

  public static Value getValue(Var var, BindingSet bindingSet) {
    return var.isConstant() ? var.getValue() : bindingSet.getValue(var.getName());
  }

  public static Set<Statement> instantiate(Collection<StatementPattern> patterns, BindingSet bindings, ValueFactory valueFactory) {
    return patterns.stream().map(pattern -> instantiate(pattern, bindings, valueFactory)).collect(Collectors.toSet());
  }

  private static Statement instantiate(StatementPattern pattern, BindingSet bindings, ValueFactory valueFactory) {
    return valueFactory.createStatement(
            (Resource) PatternEvaluator.getValue(pattern.getSubjectVar(), bindings),
            (IRI) PatternEvaluator.getValue(pattern.getPredicateVar(), bindings),
            PatternEvaluator.getValue(pattern.getObjectVar(), bindings),
            pattern.getContextVar() == null ? null : (Resource) PatternEvaluator.getValue(pattern.getContextVar(), bindings)
    );
  }
}
