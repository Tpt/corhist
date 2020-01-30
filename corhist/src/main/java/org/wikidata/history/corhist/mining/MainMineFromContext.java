package org.wikidata.history.corhist.mining;

import org.apache.commons.cli.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.corhist.dataset.Constraint;
import org.wikidata.history.corhist.dataset.ConstraintViolationCorrectionWithContext;
import org.wikidata.history.corhist.dataset.QueriesForConstraintCorrectionsBuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class MainMineFromContext {
  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  private static final Evaluation DEFAULT_EVALUATION = new Evaluation(Float.NaN, Float.NaN, 0);

  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addRequiredOption("f", "file", true, "Training data file");
    options.addRequiredOption("c", "constraints", true, "Constraints file to target");
    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    Map<IRI, Constraint> constraints = constraints(Paths.get(line.getOptionValue("constraints")));
    Map<IRI, TrainSets> setsByConstraint = setsByConstraint(
            Paths.get(line.getOptionValue("file") + ".full.train.tsv.gz"),
            Paths.get(line.getOptionValue("file") + ".full.dev.tsv.gz"),
            Paths.get(line.getOptionValue("file") + ".full.test.tsv.gz"),
            100_000,
            constraints
    );

    Path statsPath = Paths.get(line.getOptionValue("file") + ".stats.tsv");
    try (
            BufferedWriter statsWriter = Files.newBufferedWriter(statsPath)
    ) {
      statsWriter.append("constraint").append('\t')
              .append("property").append('\t')
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

      DeletionBaseline deletionBaselineComputer = new DeletionBaseline(VALUE_FACTORY);
      AdditionBaseline additionBaselineComputer = new AdditionBaseline(VALUE_FACTORY);

      setsByConstraint.entrySet().stream().parallel().forEach(constraintSets -> {
        Constraint constraint = constraints.get(constraintSets.getKey());
        TrainSets sets = constraintSets.getValue();

        Evaluation deletionBaseline = deletionBaselineComputer.compute(sets.testSet.stream().map(ConstraintViolationCorrectionWithContext::getCorrection));
        Evaluation additionBaseline = additionBaselineComputer.compute(sets.testSet.stream().map(ConstraintViolationCorrectionWithContext::getCorrection));

        //Mining and its evaluation
        Evaluation evaluation = DEFAULT_EVALUATION;
        if (!sets.trainSet.isEmpty() && !sets.testSet.isEmpty()) {
          MinerFromContext miner = new MinerFromContext();
          EvaluatorWithContext evaluator = new EvaluatorWithContext();
          TuningMinerFromContext tuningMiner = new TuningMinerFromContext(miner, evaluator);

          List<ConstraintRuleWithContext> rules = tuningMiner.mine(sets.trainSet, sets.devSet);
          evaluation = evaluator.evaluate(rules, sets.testSet);
        }

        try {
          synchronized (statsWriter) {
            statsWriter.append(constraint.getId().toString()).append('\t')
                    .append(constraint.getProperty().toString()).append('\t')
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

  private static final IRI[] PARAMETERS = new IRI[]{
          null, // id
          null, // property
          null, // type
          QueriesForConstraintCorrectionsBuilder.REGEX_PARAMETER,
          null,
          null,
          QueriesForConstraintCorrectionsBuilder.ITEM_PARAMETER,
          QueriesForConstraintCorrectionsBuilder.PROPERTY_PARAMETER,
          null,
          QueriesForConstraintCorrectionsBuilder.CLASS_PARAMETER,
          QueriesForConstraintCorrectionsBuilder.RELATION_PARAMETER
  };

  private static Map<IRI, Constraint> constraints(Path file) throws IOException {
    return Files.newBufferedReader(file, StandardCharsets.UTF_8).lines().skip(1).map(l -> {
      String[] parts = l.split("\t");
      Map<IRI, List<Value>> parameters = new HashMap<>();
      for (int i = 0; i < PARAMETERS.length; i++) {
        if (PARAMETERS[i] != null && parts.length > i) {
          String value = parts[i].trim();
          if (!value.isEmpty()) {
            List<Value> values = value.startsWith("\"")
                    ? Collections.singletonList(NTriplesUtil.parseValue(value, VALUE_FACTORY))
                    : Arrays.stream(value.split(" ")).map(v -> NTriplesUtil.parseValue(v, VALUE_FACTORY)).collect(Collectors.toList());
            parameters.put(PARAMETERS[i], values);
          }
        }
      }
      return new Constraint(NTriplesUtil.parseURI(parts[0], VALUE_FACTORY), NTriplesUtil.parseURI(parts[1], VALUE_FACTORY), NTriplesUtil.parseURI(parts[2], VALUE_FACTORY), parameters);
    }).collect(Collectors.toMap(Constraint::getId, c -> c));
  }

  private static Map<IRI, TrainSets> setsByConstraint(Path trainFile, Path devFile, Path testFile, long limit, Map<IRI, Constraint> constraints) throws IOException {
    Map<IRI, TrainSets> sets = new HashMap<>();
    newReader(trainFile)
            .lines()
            .limit(limit)
            .forEach(l -> {
              try {
                ConstraintViolationCorrectionWithContext corr = ConstraintViolationCorrectionWithContext.read(l, VALUE_FACTORY, constraints);
                sets.computeIfAbsent(corr.getCorrection().getConstraint().getId(), c -> new TrainSets()).trainSet.add(corr);
              } catch (IllegalArgumentException e) {
                //LOGGER.warn("Invalid line: " + l, e);
              }
            });
    newReader(devFile)
            .lines()
            .limit(limit)
            .forEach(l -> {
              try {
                ConstraintViolationCorrectionWithContext corr = ConstraintViolationCorrectionWithContext.read(l, VALUE_FACTORY, constraints);
                sets.computeIfAbsent(corr.getCorrection().getConstraint().getId(), c -> new TrainSets()).devSet.add(corr);
              } catch (IllegalArgumentException e) {
                //LOGGER.warn("Invalid line: " + l, e);
              }
            });
    newReader(testFile)
            .lines()
            .limit(limit)
            .forEach(l -> {
              try {
                ConstraintViolationCorrectionWithContext corr = ConstraintViolationCorrectionWithContext.read(l, VALUE_FACTORY, constraints);
                sets.computeIfAbsent(corr.getCorrection().getConstraint().getId(), c -> new TrainSets()).testSet.add(corr);
              } catch (IllegalArgumentException e) {
                //LOGGER.warn("Invalid line: " + l, e);
              }
            });
    return sets;
  }

  private static BufferedReader newReader(Path path) throws IOException {
    if (path.toString().endsWith(".gz")) {
      return new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path))));
    } else {
      return Files.newBufferedReader(path);
    }
  }

  private static final class TrainSets {
    private final List<ConstraintViolationCorrectionWithContext> trainSet = new ArrayList<>();
    private final List<ConstraintViolationCorrectionWithContext> devSet = new ArrayList<>();
    private final List<ConstraintViolationCorrectionWithContext> testSet = new ArrayList<>();
  }
}