package org.wikidata.history.mining;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

  private static final double TRAIN_SET_RATIO = 0.8;
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  private static final Evaluation DEFAULT_EVALUATION = new Evaluation(Float.NaN, Float.NaN, 0);

  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addOption("c", "constraints", true, "Constraints to  target");
    options.addOption("l", "limit", true, "Number of corrections to get per correction seed pattern");
    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    Path index = Paths.get("wd-history-index");
    String filter = line.getOptionValue("constraints", "*");
    String limitStr = line.getOptionValue("limit", "");
    OptionalLong limit = limitStr.isEmpty() ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(limitStr));
    String qualifiedFilter = filter + (limit.isPresent() ? "-" + limit.getAsLong() : "");

    try (HistoryRepository repository = new HistoryRepository(index)) {
      //Read constraints
      Collection<Constraint> constraints = new ConstraintsListBuilder().build();

      //Read or write corrections
      Path correctionsFile = Paths.get("constraint-corrections-" + qualifiedFilter + ".tsv");
      ConstraintViolationCorrectionLookup constraintViolationCorrectionLookup = new ConstraintViolationCorrectionLookup(filter, repository, limit);
      Stream<Map.Entry<Constraint, TrainAndTestSets>> corrections = Files.exists(correctionsFile)
              ? readCorrectionsFile(correctionsFile, repository.getValueFactory(), constraints)
              : findAndSaveCorrections(correctionsFile, constraintViolationCorrectionLookup, constraints);

      System.out.println("Starting to learn corrections rules");
      try (BufferedWriter statsWriter = Files.newBufferedWriter(Paths.get("constraint-stats-" + qualifiedFilter + ".tsv"))) {
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
                .append("deletion baseline precision").append('\t')
                .append("deletion baseline recall").append('\t')
                .append("deletion baseline F-1").append('\t')
                .append("addition baseline precision").append('\t')
                .append("addition baseline recall").append('\t')
                .append("addition baseline F-1")
                .append('\n');
        List<ConstraintRule> allRules = new ArrayList<>();
        DeletionBaseline deletionBaselineComputer = new DeletionBaseline(repository.getValueFactory());
        AdditionBaseline additionBaselineComputer = new AdditionBaseline(repository.getValueFactory());
        corrections.filter(data -> !data.getValue().isEmpty())
                .map(data -> {
                  Constraint constraint = data.getKey();
                  TrainAndTestSets sets = data.getValue();
                  long currentInstancesCount = constraintViolationCorrectionLookup.countCurrentInstances(constraint);
                  long currentViolationsCount = constraintViolationCorrectionLookup.countCurrentViolations(constraint);
                  Map<Pair<Long, Long>, Long> correctedViolations = correctionsPerAdditionsDeletions(sets.stream());
                  long oneAddition = correctedViolations.getOrDefault(Pair.of(1L, 0L), 0L);
                  long oneDeletion = correctedViolations.getOrDefault(Pair.of(0L, 1L), 0L);
                  long oneReplacement = correctedViolations.getOrDefault(Pair.of(1L, 1L), 0L);
                  long otherCorrections = moreThanTwoChanges(correctedViolations);
                  Evaluation deletionBaseline = deletionBaselineComputer.compute(Stream.concat(sets.trainSet.stream(), sets.testSet.stream()));
                  Evaluation additionBaseline = additionBaselineComputer.compute(Stream.concat(sets.trainSet.stream(), sets.testSet.stream()));

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
                              .append(Integer.toString(evaluation.getTestSetSize())).append('\t')
                              .append(Float.toString(deletionBaseline.getPrecision())).append('\t')
                              .append(Float.toString(deletionBaseline.getRecall())).append('\t')
                              .append(Float.toString(deletionBaseline.getF1())).append('\t')
                              .append(Float.toString(additionBaseline.getPrecision())).append('\t')
                              .append(Float.toString(additionBaseline.getRecall())).append('\t')
                              .append(Float.toString(additionBaseline.getF1()))
                              .append('\n');
                    } catch (IOException e) {
                      LOGGER.error(e.getMessage(), e);
                    }
                  }

                  return Pair.of(
                          Pair.of(
                                  Triple.of(1, currentInstancesCount, currentViolationsCount),
                                  correctedViolations
                          ),
                          Triple.of(
                                  Pair.of(evaluation, evaluation)
                                  , Pair.of(deletionBaseline, deletionBaseline)
                                  , Pair.of(additionBaseline, additionBaseline)
                          )
                  );
                })
                .reduce((e1, e2) -> Pair.of(
                        Pair.of(
                                Triple.of(
                                        e1.getLeft().getLeft().getLeft() + e2.getLeft().getLeft().getLeft(),
                                        e1.getLeft().getLeft().getMiddle() + e2.getLeft().getLeft().getMiddle(),
                                        e1.getLeft().getLeft().getRight() + e2.getLeft().getLeft().getRight()
                                ),
                                add(e1.getLeft().getRight(), e2.getLeft().getRight())
                        ),
                        Triple.of(
                                weightedAndAverageCombination(e1.getRight().getLeft(), e2.getRight().getLeft()),
                                weightedAndAverageCombination(e1.getRight().getMiddle(), e2.getRight().getMiddle()),
                                weightedAndAverageCombination(e1.getRight().getRight(), e2.getRight().getRight())
                        )
                )).ifPresent(stats -> {
          long constraintsCount = stats.getLeft().getLeft().getLeft();
          long currentInstancesCount = stats.getLeft().getLeft().getMiddle();
          long currentViolationsCount = stats.getLeft().getLeft().getRight();
          Map<Pair<Long, Long>, Long> correctedViolations = stats.getLeft().getRight();
          Evaluation evalWeighted = stats.getRight().getLeft().getLeft();
          Evaluation evalAverage = stats.getRight().getLeft().getRight();
          Evaluation evalDeletionBaselineWeighted = stats.getRight().getMiddle().getLeft();
          Evaluation evalDeletionBaselineAverage = stats.getRight().getMiddle().getRight();
          Evaluation evalAdditionBaselineWeighted = stats.getRight().getRight().getLeft();
          Evaluation evalAdditionBaselineAverage = stats.getRight().getRight().getRight();
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
                          evalAverage.getPrecision() + " average precision, " +
                          evalAverage.getRecall() + " average recall, " +
                          evalAverage.getF1() + " average F-1," +
                          evalDeletionBaselineWeighted.getPrecision() + " deletion baseline weighted precision, " +
                          evalDeletionBaselineWeighted.getRecall() + " deletion baseline weighted recall, " +
                          evalDeletionBaselineWeighted.getF1() + " deletion baseline weighted F-1, " +
                          evalDeletionBaselineAverage.getPrecision() + " deletion baseline average precision, " +
                          evalDeletionBaselineAverage.getRecall() + " deletion baseline average recall, " +
                          evalDeletionBaselineAverage.getF1() + " deletion baseline average F-1, " +
                          evalAdditionBaselineWeighted.getPrecision() + " addition baseline weighted precision, " +
                          evalAdditionBaselineWeighted.getRecall() + " addition baseline weighted recall, " +
                          evalAdditionBaselineWeighted.getF1() + " addition baseline weighted F-1, " +
                          evalAdditionBaselineAverage.getPrecision() + " addition baseline average precision, " +
                          evalAdditionBaselineAverage.getRecall() + " addition baseline average recall, " +
                          evalAdditionBaselineAverage.getF1() + " addition baseline average F-1."
          );
        });

        try (
                ObjectOutputStream serializedOutputStream = new ObjectOutputStream(Files.newOutputStream(Paths.get("constraint-rules-" + qualifiedFilter + ".ser")));
                BufferedWriter textWriter = Files.newBufferedWriter(Paths.get("constraint-rules-" + qualifiedFilter + ".txt"))
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
  }

  private static Pair<Evaluation, Evaluation> weightedAndAverageCombination(Pair<Evaluation, Evaluation> e1, Pair<Evaluation, Evaluation> e2) {
    return Pair.of(
            weightedCombination(e1.getLeft(), e2.getLeft()),
            averageCombination(e1.getRight(), e2.getRight())
    );
  }

  private static Evaluation weightedCombination(Evaluation e1, Evaluation e2) {
    return new Evaluation(
            weightedAverage(e1.getPrecision(), e1.getTestSetSize(), e2.getPrecision(), e2.getTestSetSize()),
            weightedAverage(e1.getRecall(), e1.getTestSetSize(), e2.getRecall(), e2.getTestSetSize()),
            e1.getTestSetSize() + e2.getTestSetSize()
    );
  }

  private static Evaluation averageCombination(Evaluation e1, Evaluation e2) {
    return new Evaluation(
            weightedAverage(e1.getPrecision(), 1, e2.getPrecision(), 1),
            weightedAverage(e1.getRecall(), 1, e2.getRecall(), 1),
            e1.getTestSetSize() + e2.getTestSetSize()
    );
  }

  private static Stream<Map.Entry<Constraint, TrainAndTestSets>> readCorrectionsFile(Path file, ValueFactory valueFactory, Collection<Constraint> constraints) throws IOException {
    System.out.println("Reading corrections dataset from " + file);
    Map<IRI, Constraint> constraintsMap = constraints.stream().collect(Collectors.toMap(Constraint::getId, Function.identity()));
    Map<Constraint, TrainAndTestSets> setsForConstraint = new HashMap<>();
    try (Stream<String> lines = Files.lines(file)) {
      lines.forEach(line -> {
                try {
                  ConstraintViolationCorrection correction = ConstraintViolationCorrection.read(line, valueFactory, constraintsMap);
                  setsForConstraint.computeIfAbsent(correction.getConstraint(), (k) -> new TrainAndTestSets()).add(correction);
                } catch (IllegalArgumentException e) {
                  LOGGER.warn(e.getMessage(), e);
                }
              }
      );
    }
    System.out.println("Corrections dataset reading done");

    return setsForConstraint.entrySet().stream();
  }

  private static Stream<Map.Entry<Constraint, TrainAndTestSets>> findAndSaveCorrections(Path file, ConstraintViolationCorrectionLookup constraintViolationCorrectionLookup, Collection<Constraint> constraints) throws IOException {
    System.out.println("Mining corrections dataset");
    Map<Constraint, TrainAndTestSets> setsForConstraint = constraints.stream().map(constraint -> {
      TrainAndTestSets sets = new TrainAndTestSets();
      constraintViolationCorrectionLookup.findCorrections(constraint).forEach(sets::add);
      return Pair.of(constraint, sets);
    }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));


    System.out.println("Saving corrections dataset to " + file);
    try (BufferedWriter correctionsWriter = Files.newBufferedWriter(file)) {
      setsForConstraint.values().stream().flatMap(TrainAndTestSets::stream).forEach(correction -> {
        try {
          correction.write(correctionsWriter);
        } catch (IOException e) {
          LOGGER.error(e.getMessage(), e);
        }
      });
    }
    System.out.println("Corrections dataset saving done");

    return setsForConstraint.entrySet().stream();
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
    private final List<ConstraintViolationCorrection> trainSet = new ArrayList<>();
    private final List<ConstraintViolationCorrection> testSet = new ArrayList<>();

    private void add(ConstraintViolationCorrection correction) {
      if (Math.random() >= TRAIN_SET_RATIO) {
        testSet.add(correction);
      } else {
        trainSet.add(correction);
      }
    }

    private Stream<ConstraintViolationCorrection> stream() {
      return Stream.concat(trainSet.stream(), testSet.stream());
    }

    private boolean isEmpty() {
      return trainSet.isEmpty() && testSet.isEmpty();
    }
  }
}
