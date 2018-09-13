package org.wikidata.history.mining;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.dataset.Constraint;
import org.wikidata.history.dataset.ConstraintViolationCorrection;
import org.wikidata.history.dataset.ConstraintViolationCorrectionLookup;
import org.wikidata.history.dataset.ConstraintsListBuilder;
import org.wikidata.history.sparql.HistoryRepository;
import org.wikidata.history.sparql.Vocabulary;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

  private static final double TRAIN_SET_RATIO = 0.8;
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  private static final Evaluation DEFAULT_EVALUATION = new Evaluation(Float.NaN, Float.NaN, 0);

  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addOption("c", "constraints", true, "Constraints to  target");
    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    Path index = Paths.get("wd-history-index");
    String filter = line.getOptionValue("constraints", "*");
    try (
            HistoryRepository repository = new HistoryRepository(index);
            BufferedWriter statsWriter = Files.newBufferedWriter(Paths.get("constraint-stats-" + filter + ".tsv"))
    ) {
      statsWriter.append("constraint").append('\t')
              .append("property").append('\t')
              .append("property instances").append('\t')
              .append("current violations").append('\t')
              .append("corrections with one addition").append('\t')
              .append("corrections with one deletion").append('\t')
              .append("corrections with one replacement").append('\t')
              .append("other corrections").append('\t')
              .append("mined precision").append('\t')
              .append("mined recall").append('\t')
              .append("mined F-1").append('\t')
              .append("test set size").append('\t')
              .append('\n');
      ConstraintViolationCorrectionLookup constraintViolationCorrectionLookup = new ConstraintViolationCorrectionLookup(filter, repository);
      List<ConstraintRule> allRules = new ArrayList<>();
      new ConstraintsListBuilder().build().parallelStream()
              .map(constraint -> ImmutablePair.of(constraint, buildTrainAndTestSet(constraintViolationCorrectionLookup, constraint)))
              .filter(data -> !data.getRight().isEmpty())
              .map(data -> {
                Constraint constraint = data.getLeft();
                TrainAndTestSets sets = data.getRight();
                long currentInstancesCount = constraintViolationCorrectionLookup.countCurrentInstances(constraint);
                long currentViolationsCount = constraintViolationCorrectionLookup.countCurrentViolations(constraint);
                Map<Pair<Long, Long>, Long> correctedViolations = correctionsPerAdditionsDeletions(sets.stream());
                long oneAddition = correctedViolations.getOrDefault(Pair.of(1L, 0L), 0L);
                long oneDeletion = correctedViolations.getOrDefault(Pair.of(0L, 1L), 0L);
                long oneReplacement = correctedViolations.getOrDefault(Pair.of(1L, 1L), 0L);
                long otherCorrections = moreThanTwoChanges(correctedViolations);

                //Mining and its evaluation
                Evaluation evaluation = DEFAULT_EVALUATION;
                if (!sets.trainSet.isEmpty() && !sets.testSet.isEmpty()) {
                  try (RepositoryConnection connection = repository.getConnection()) {
                    Miner miner = new Miner(connection);
                    Evaluator evaluator = new Evaluator(connection);
                    TuningMiner tuningMiner = new TuningMiner(miner, evaluator);

                    List<ConstraintRule> rules = tuningMiner.mine(sets.trainSet);
                    allRules.addAll(rules);
                    evaluation = evaluator.evaluate(rules, sets.testSet);
                  }
                }

                synchronized (statsWriter) {
                  try {
                    statsWriter.append(constraint.getId().toString()).append('\t')
                            .append(constraint.getProperty().toString()).append('\t')
                            .append(Long.toString(currentInstancesCount)).append('\t')
                            .append(Long.toString(currentViolationsCount)).append('\t')
                            .append(Long.toString(oneAddition)).append('\t')
                            .append(Long.toString(oneDeletion)).append('\t')
                            .append(Long.toString(oneReplacement)).append('\t')
                            .append(Long.toString(otherCorrections)).append('\t')
                            .append(Float.toString(evaluation.getPrecision())).append('\t')
                            .append(Float.toString(evaluation.getRecall())).append('\t')
                            .append(Float.toString(evaluation.getF1())).append('\t')
                            .append(Integer.toString(evaluation.getTestSetSize())).append('\n');
                  } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                  }
                }

                return ImmutableTriple.of(
                        ImmutablePair.of(
                                ImmutableTriple.of(1, currentInstancesCount, currentViolationsCount),
                                correctedViolations
                        ),
                        evaluation,
                        evaluation
                );
              })
              .reduce((e1, e2) -> {
                Evaluation evalWeighted1 = e1.getMiddle();
                Evaluation evalEqual1 = e1.getRight();
                Evaluation evalWeighted2 = e2.getMiddle();
                Evaluation evalEqual2 = e2.getRight();
                return ImmutableTriple.of(
                        ImmutablePair.of(
                                ImmutableTriple.of(
                                        e1.getLeft().getLeft().getLeft() + e2.getLeft().getLeft().getLeft(),
                                        e1.getLeft().getLeft().getMiddle() + e2.getLeft().getLeft().getMiddle(),
                                        e1.getLeft().getLeft().getRight() + e2.getLeft().getLeft().getRight()
                                ),
                                add(e1.getLeft().getRight(), e2.getLeft().getRight())
                        ),
                        new Evaluation(
                                weightedAverage(evalWeighted1.getPrecision(), evalWeighted1.getTestSetSize(), evalWeighted2.getPrecision(), evalWeighted2.getTestSetSize()),
                                weightedAverage(evalWeighted1.getRecall(), evalWeighted1.getTestSetSize(), evalWeighted2.getRecall(), evalWeighted2.getTestSetSize()),
                                evalWeighted1.getTestSetSize() + evalWeighted2.getTestSetSize()
                        ),
                        new Evaluation(
                                weightedAverage(evalEqual1.getPrecision(), 1, evalEqual2.getPrecision(), 1),
                                weightedAverage(evalEqual1.getRecall(), 1, evalEqual2.getRecall(), 1),
                                evalEqual1.getTestSetSize() + evalEqual2.getTestSetSize()
                        )
                );
              }).ifPresent(stats -> {
        long constraintsCount = stats.getLeft().getLeft().getLeft();
        long currentInstancesCount = stats.getLeft().getLeft().getMiddle();
        long currentViolationsCount = stats.getLeft().getLeft().getRight();
        Map<Pair<Long, Long>, Long> correctedViolations = stats.getLeft().getRight();
        Evaluation evalWeighted = stats.getMiddle();
        Evaluation evalEqual = stats.getRight();
        System.out.println(
                "Aggregated stats: " +
                        constraintsCount + " constraints, " +
                        currentInstancesCount + " current instances, " +
                        currentViolationsCount + " current violations, " +
                        correctedViolations.getOrDefault(Pair.of(1L, 0L), 0L) + " solved violations with one addition, " +
                        correctedViolations.getOrDefault(Pair.of(0L, 1L), 0L) + " solved violations with one deletion, " +
                        correctedViolations.getOrDefault(Pair.of(1L, 1L), 0L) + " solved violations with one replacement, " +
                        moreThanTwoChanges(correctedViolations) + " solved violations with an other diff, " +
                        evalWeighted.getPrecision() + " weighted precision, " +
                        evalWeighted.getRecall() + " weighted recall, " +
                        evalWeighted.getF1() + " weighted F-1, " +
                        evalEqual.getPrecision() + " raw precision, " +
                        evalEqual.getRecall() + " raw recall, " +
                        evalEqual.getF1() + " raw F-1."
        );
      });

      try (
              ObjectOutputStream serializedOutputStream = new ObjectOutputStream(Files.newOutputStream(Paths.get("constraint-rules-" + filter + ".ser")));
              BufferedWriter textWriter = Files.newBufferedWriter(Paths.get("constraint-rules-" + filter + ".txt"))
      ) {
        allRules.sort(Comparator.reverseOrder());
        for (ConstraintRule rule : allRules) {
          serializedOutputStream.writeObject(rule.toSimple());
          textWriter.write(
                  rule.getHead().stream().map(Main::toString).collect(Collectors.joining("\t")) + "\t<-\t" +
                          toString(rule.getViolationBody()) + "\t" +
                          rule.getContextBody().stream().map(Main::toString).collect(Collectors.joining("\t")) + "\t" +
                          rule.getStdConfidence() + "\t" +
                          rule.getSupport() + "\n"
          );
        }
      }
    }
  }

  private static TrainAndTestSets buildTrainAndTestSet(ConstraintViolationCorrectionLookup lookup, Constraint constraint) {
    List<ConstraintViolationCorrection> trainSet = new ArrayList<>();
    List<ConstraintViolationCorrection> testSet = new ArrayList<>();
    lookup.findCorrections(constraint).forEach(correction -> {
      if (Math.random() >= TRAIN_SET_RATIO) {
        testSet.add(correction);
      } else {
        trainSet.add(correction);
      }
    });
    return new TrainAndTestSets(trainSet, testSet);
  }

  private static float weightedAverage(float v1, int p1, float v2, int p2) {
    if (Float.isNaN(v1)) {
      return v2;
    }
    if (Float.isNaN(v2)) {
      return v1;
    }
    return (v1 * p1 + v2 * p2) / (p1 + p2);
  }

  private static <K> Map<K, Long> add(Map<K, Long> a, Map<K, Long> b) {
    for (Map.Entry<K, Long> entry : b.entrySet()) {
      a.put(entry.getKey(), a.getOrDefault(entry.getKey(), 0L) + entry.getValue());
    }
    return a;
  }

  private static Map<Pair<Long, Long>, Long> correctionsPerAdditionsDeletions(Stream<ConstraintViolationCorrection> corrections) {
    Map<Pair<Long, Long>, Long> result = new HashMap<>();
    corrections.forEach(correction -> {
      if (correction.getCorrection().isEmpty()) {
        System.out.println("Wrong correction: " + correction);
        return;
      }

      long add = 0;
      long del = 0;
      for (Statement statement : correction.getCorrection()) {
        if (Vocabulary.HISTORY_DELETION.equals(statement.getContext())) {
          del++;
        } else if (Vocabulary.HISTORY_ADDITION.equals(statement.getContext())) {
          add++;
        } else {
          System.out.println("Wrong correction statement: " + statement);
        }
      }
      Pair<Long, Long> key = Pair.of(add, del);
      result.put(key, result.getOrDefault(key, 0L) + 1);
    });
    return result;
  }

  private static long moreThanTwoChanges(Map<Pair<Long, Long>, Long> input) {
    return input.entrySet().stream()
            .filter(pair -> pair.getKey().getLeft() > 1 || pair.getKey().getRight() > 1)
            .map(Map.Entry::getValue)
            .reduce(0L, (a, b) -> a + b);
  }

  private static String toString(StatementPattern pattern) {
    StringBuilder builder = new StringBuilder();
    if (pattern.getSubjectVar().isConstant()) {
      builder.append(pattern.getSubjectVar().getValue());
    } else {
      builder.append('?').append(pattern.getSubjectVar().getName());
    }
    builder.append(' ');
    if (pattern.getPredicateVar().isConstant()) {
      builder.append(pattern.getPredicateVar().getValue());
    } else {
      builder.append('?').append(pattern.getPredicateVar().getName());
    }
    builder.append(' ');
    if (pattern.getObjectVar().isConstant()) {
      builder.append(pattern.getObjectVar().getValue());
    } else {
      builder.append('?').append(pattern.getObjectVar().getName());
    }
    builder.append(' ');
    if (pattern.getContextVar() != null) {
      if (pattern.getContextVar().isConstant()) {
        builder.append(pattern.getContextVar().getValue());
      } else {
        builder.append('?').append(pattern.getContextVar().getName());
      }
    }
    return builder.toString();
  }

  private static final class TrainAndTestSets {
    private final List<ConstraintViolationCorrection> trainSet;
    private final List<ConstraintViolationCorrection> testSet;

    private TrainAndTestSets(List<ConstraintViolationCorrection> trainSet, List<ConstraintViolationCorrection> testSet) {
      this.trainSet = trainSet;
      this.testSet = testSet;
    }

    private Stream<ConstraintViolationCorrection> stream() {
      return Stream.concat(trainSet.stream(), testSet.stream());
    }

    private boolean isEmpty() {
      return trainSet.isEmpty() && testSet.isEmpty();
    }
  }
}
