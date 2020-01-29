package org.wikidata.history.corhist.mining;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.TupleExprs;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.wikidata.history.corhist.dataset.ConstraintViolationCorrectionWithContext;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ConstraintRuleWithContext implements Comparable<ConstraintRuleWithContext> {

  private final Set<StatementPattern> head;
  private final StatementPattern violationBody;
  private final List<ContextPattern> contextPatterns;
  private final List<ContextualBinding> bodyBindings;
  private final List<ContextualBinding> fullBindings;

  ConstraintRuleWithContext(Set<StatementPattern> head, StatementPattern violationBody, List<ContextPattern> contextPatterns, List<ContextualBinding> bodyBindings, List<ContextualBinding> fullBindings) {
    this.head = head;
    this.violationBody = violationBody;
    this.contextPatterns = contextPatterns;
    this.bodyBindings = bodyBindings;
    this.fullBindings = fullBindings;
  }

  ConstraintRuleWithContext(Set<StatementPattern> head, StatementPattern violationBody, List<ContextualBinding> bodyBindings, List<ContextualBinding> fullBindings) {
    this(head, violationBody, Collections.emptyList(), bodyBindings, fullBindings);
  }

  public Set<StatementPattern> getHead() {
    return head;
  }

  public StatementPattern getViolationBody() {
    return violationBody;
  }

  public List<StatementPattern> getContextBody() {
    return contextPatterns.stream().map(ContextPattern::toPattern).collect(Collectors.toList());
  }

  public List<ContextPattern> getContextPatterns() {
    return contextPatterns;
  }

  Stream<ContextualBinding> getBodyBindings() {
    return bodyBindings.stream();
  }

  Stream<ContextualBinding> getFullBindings() {
    return fullBindings.stream();
  }

  public int getSupport() {
    return fullBindings.size();
  }

  public float getStdConfidence() {
    return fullBindings.size() / (float) bodyBindings.size();
  }

  @Override
  public int compareTo(ConstraintRuleWithContext r) {
    int comp = Float.compare(getStdConfidence(), r.getStdConfidence());
    return (comp == 0) ? Integer.compare(fullBindings.size(), r.fullBindings.size()) : comp;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ConstraintRuleWithContext)) {
      return false;
    }
    ConstraintRuleWithContext o = (ConstraintRuleWithContext) other;
    return head.equals(o.head) && violationBody.equals(o.violationBody) && contextPatterns.equals(o.contextPatterns);
  }

  @Override
  public int hashCode() {
    return head.hashCode() ^ violationBody.hashCode();
  }

  public static class ContextPattern {
    public final String subjectVariable;
    public final IRI predicate;
    public final Value object;
    public final String objectVariable;

    public ContextPattern(String subjectVariable, IRI predicate, Value object) {
      this.subjectVariable = subjectVariable;
      this.predicate = predicate;
      this.object = object;
      this.objectVariable = null;
    }

    public ContextPattern(String subjectVariable, IRI predicate, String objectVariable) {
      this.subjectVariable = subjectVariable;
      this.predicate = predicate;
      this.object = null;
      this.objectVariable = objectVariable;
    }


    public StatementPattern toPattern() {
      return new StatementPattern(
              new Var(subjectVariable),
              TupleExprs.createConstVar(predicate),
              objectVariable == null
                      ? (object == null ? new Var(Integer.toString(Math.abs(hashCode()))) : TupleExprs.createConstVar(object))
                      : new Var(objectVariable)
      );
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ContextPattern)) {
        return false;
      }
      ContextPattern o = (ContextPattern) other;
      return Objects.equals(subjectVariable, o.subjectVariable) && Objects.equals(predicate, o.predicate) && Objects.equals(object, o.object) && Objects.equals(objectVariable, o.objectVariable);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(subjectVariable) ^ Objects.hashCode(predicate) ^ Objects.hashCode(object) ^ Objects.hashCode(objectVariable);
    }

    @Override
    public String toString() {
      if (objectVariable != null) {
        return "?" + subjectVariable + " " + NTriplesUtil.toNTriplesString(predicate) + " ?" + objectVariable;
      } else if (object != null) {
        return "?" + subjectVariable + " " + NTriplesUtil.toNTriplesString(predicate) + " " + NTriplesUtil.toNTriplesString(object);
      } else {
        return "?" + subjectVariable + " " + NTriplesUtil.toNTriplesString(predicate) + " ?" + Math.abs(hashCode());
      }
    }
  }

  public static class ContextualBinding {
    private final Value s;
    private final Value o;
    private final Value otherS;
    private final Value otherO;
    public final ConstraintViolationCorrectionWithContext correction;

    public ContextualBinding(ConstraintViolationCorrectionWithContext correction) {
      this(correction, null, null, null, null);
    }

    public ContextualBinding(ConstraintViolationCorrectionWithContext correction, Value s, Value o, Value otherS, Value otherO) {
      this.correction = correction;
      this.s = s;
      this.o = o;
      this.otherS = otherS;
      this.otherO = otherO;
    }

    public Value getValue(String varName) {
      switch (varName) {
        case "s":
          return s;
        case "o":
          return o;
        case "otherS":
          return otherS;
        case "otherO":
          return otherO;
        default:
          throw new IllegalArgumentException("Unexpected variable: " + varName);
      }
    }

    public ContextualBinding withValue(String varName, Value value) {
      switch (varName) {
        case "s":
          if (value.equals(s)) {
            return this;
          } else if (s == null) {
            return new ContextualBinding(correction, value, o, otherS, otherO);
          } else {
            throw new IllegalArgumentException("s is already set");
          }
        case "o":
          if (value.equals(o)) {
            return this;
          } else if (o == null) {
            return new ContextualBinding(correction, s, value, otherS, otherO);
          } else {
            throw new IllegalArgumentException("o is already set");
          }
        case "otherS":
          if (value.equals(otherS)) {
            return this;
          } else if (otherS == null) {
            return new ContextualBinding(correction, s, o, value, otherO);
          } else {
            throw new IllegalArgumentException("otherS is already set");
          }
        case "otherO":
          if (value.equals(otherO)) {
            return this;
          } else if (otherO == null) {
            return new ContextualBinding(correction, s, o, otherS, value);
          } else {
            throw new IllegalArgumentException("otherO is already set");
          }
        default:
          throw new IllegalArgumentException("Unexpected variable: " + varName);
      }
    }

    public boolean matches(ContextPattern pattern) {
      Value subject = getValue(pattern.subjectVariable);
      Value object = (pattern.objectVariable != null) ? getValue(pattern.objectVariable) : pattern.object;
      if (subject == null) {
        if (object == null) {
          return !correction.getPsoContext()
                  .getOrDefault(pattern.predicate, Collections.emptyList())
                  .isEmpty();
        } else {
          return correction.getPsoContext()
                  .getOrDefault(pattern.predicate, Collections.emptyList())
                  .stream()
                  .anyMatch(e -> e.getValue().equals(object));
        }
      } else {
        if (object == null) {
          return correction.getPsoContext()
                  .getOrDefault(pattern.predicate, Collections.emptyList())
                  .stream()
                  .anyMatch(e -> e.getKey().equals(subject));
        } else {
          return correction.getPsoContext()
                  .getOrDefault(pattern.predicate, Collections.emptyList())
                  .contains(Map.entry(subject, object));
        }
      }
    }

    public Stream<ContextualBinding> evaluate(ContextPattern pattern) {
      Value subject = getValue(pattern.subjectVariable);
      Value object = (pattern.objectVariable != null) ? getValue(pattern.objectVariable) : pattern.object;
      return correction.getPsoContext()
              .getOrDefault(pattern.predicate, Collections.emptyList())
              .stream()
              .filter(so -> (subject == null || subject.equals(so.getKey())) && (object == null || object.equals(so.getValue())))
              .map(so -> {
                ContextualBinding result = this.withValue(pattern.subjectVariable, so.getKey());
                if (pattern.objectVariable != null) {
                  result = result.withValue(pattern.objectVariable, so.getValue());
                }
                return result;
              });
    }
  }
}
