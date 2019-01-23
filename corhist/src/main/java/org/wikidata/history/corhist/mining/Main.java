package org.wikidata.history.corhist.mining;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.corhist.dataset.Constraint;
import org.wikidata.history.corhist.dataset.ConstraintViolationCorrection;
import org.wikidata.history.corhist.dataset.ConstraintViolationCorrectionLookup;
import org.wikidata.history.corhist.dataset.ConstraintsListBuilder;
import org.wikidata.history.sparql.HistoryRepository;
import org.wikidata.history.sparql.Vocabulary;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

  private static final double TRAIN_SET_RATIO = 0.8;
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  private static final Evaluation DEFAULT_EVALUATION = new Evaluation(Float.NaN, Float.NaN, 0);
  private static final OpenOption[] CREATE_AND_APPEND = new OpenOption[]{StandardOpenOption.APPEND, StandardOpenOption.CREATE};

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

    Path statsPath = Paths.get("constraint-stats-" + qualifiedFilter + ".tsv");
    Set<String> alreadyDone = alreadyDoneConstraints(statsPath);
    try (
            HistoryRepository repository = new HistoryRepository(index);
            BufferedWriter statsWriter = Files.newBufferedWriter(statsPath, CREATE_AND_APPEND);
            ObjectOutputStream serializedRulesOutputStream = new ObjectOutputStream(Files.newOutputStream(Paths.get("constraint-rules-" + qualifiedFilter + ".ser"), CREATE_AND_APPEND));
            BufferedWriter rulesTextWriter = Files.newBufferedWriter(Paths.get("constraint-rules-" + qualifiedFilter + ".txt"), CREATE_AND_APPEND)
    ) {
      if (alreadyDone.isEmpty()) {
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
      }

      //Read constraints
      Collection<Constraint> constraints = new ConstraintsListBuilder().build();
      ConstraintViolationCorrectionLookup constraintViolationCorrectionLookup = new ConstraintViolationCorrectionLookup(filter, repository, limit);
      DeletionBaseline deletionBaselineComputer = new DeletionBaseline(repository.getValueFactory());
      AdditionBaseline additionBaselineComputer = new AdditionBaseline(repository.getValueFactory());

      Path correctionsDir = Paths.get("constraint-corrections-" + qualifiedFilter);
      try {
        Files.createDirectory(correctionsDir);
      } catch (FileAlreadyExistsException e) {
      }

      constraints.stream().parallel().forEach(constraint -> {
        if (alreadyDone.contains(constraint.getId().stringValue())) {
          return;
        }
        long currentInstancesCount = constraintViolationCorrectionLookup.countCurrentInstances(constraint);
        if (currentInstancesCount == 0) {
          return; //Filtered out constraint
        }

        try {
          Path correctionsFile = correctionsDir.resolve(constraint.getId().getLocalName());
          TrainAndTestSets sets = Files.exists(correctionsFile)
                  ? readCorrectionsFile(correctionsFile, repository.getValueFactory(), constraint)
                  : findAndSaveCorrections(correctionsFile, constraintViolationCorrectionLookup, constraint);

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
              evaluation = evaluator.evaluate(rules, sets.testSet);

              //Save rules
              rules.sort(Comparator.reverseOrder());
              for (ConstraintRule rule : rules) {
                try {
                  serializedRulesOutputStream.writeObject(rule.toSimple());
                  rulesTextWriter.write(
                          rule.getHead().stream().map(Main::toString).collect(Collectors.joining("\t")) + "\t<-\t" +
                                  toString(rule.getViolationBody()) + "\t" +
                                  rule.getContextBody().stream().map(Main::toString).collect(Collectors.joining("\t")) + "\t" +
                                  rule.getStdConfidence() + "\t" +
                                  rule.getSupport() + "\n"
                  );
                } catch (Exception e) {
                  LOGGER.error(e.getMessage(), e);
                }
              }
              rulesTextWriter.flush();
            }
          }

          synchronized (statsWriter) {
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
            statsWriter.flush();
          }


        } catch (IOException e) {
          LOGGER.error(e.getMessage(), e);
        }
      });
    }
  }

  private static TrainAndTestSets readCorrectionsFile(Path file, ValueFactory valueFactory, Constraint constraint) throws IOException {
    Map<IRI, Constraint> constraintsMap = Collections.singletonMap(constraint.getId(), constraint);
    TrainAndTestSets set = new TrainAndTestSets();
    try (Stream<String> lines = Files.lines(file)) {
      lines.forEach(line -> {
                try {
                  ConstraintViolationCorrection correction = ConstraintViolationCorrection.read(line, valueFactory, constraintsMap);
                  set.add(correction);
                } catch (IllegalArgumentException e) {
                  LOGGER.warn(e.getMessage(), e);
                }
              }
      );
    }
    return set;
  }

  private static TrainAndTestSets findAndSaveCorrections(Path file, ConstraintViolationCorrectionLookup constraintViolationCorrectionLookup, Constraint constraint) throws IOException {
    TrainAndTestSets set = new TrainAndTestSets();
    constraintViolationCorrectionLookup.findCorrections(constraint).forEach(set::add);

    try (BufferedWriter correctionsWriter = Files.newBufferedWriter(file)) {
      set.stream().forEach(correction -> {
        try {
          correction.write(correctionsWriter);
        } catch (IOException e) {
          LOGGER.error(e.getMessage(), e);
        }
      });
    }

    return set;
  }

  private static Set<String> alreadyDoneConstraints(Path statsFile) throws IOException {
    if (!Files.exists(statsFile)) {
      return Collections.emptySet();
    }
    try (Stream<String> lines = Files.lines(statsFile)) {
      return lines.skip(1).map(l -> l.split("\t")[0]).collect(Collectors.toSet());
    }

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

    boolean isEmpty() {
      return trainSet.isEmpty() && testSet.isEmpty();
    }
  }
}
