package org.wikidata.history.corhist.dataset;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.sparql.HistoryRepository;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger("dataset");

  public static void main(String[] args) throws IOException, ParseException {
    Options options = new Options();
    options.addOption("c", "constraints", true, "Constraints to  target");
    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    Path index = Paths.get("wd-history-index");
    String filter = line.getOptionValue("constraints", "*");
    try (
            HistoryRepository historyRepository = new HistoryRepository(index);
            BufferedWriter writer = Files.newBufferedWriter(Paths.get("constraint-corrections-" + filter + ".tsv"))
    ) {
      ConstraintViolationCorrectionLookup constraintViolationCorrectionLookup = new ConstraintViolationCorrectionLookup(filter, historyRepository);
      new ConstraintsListBuilder().build().stream()
              .flatMap(constraintViolationCorrectionLookup::findCorrections)
              .forEach(correction -> {
                try {
                  correction.write(writer);
                } catch (IOException e) {
                  LOGGER.error(e.getMessage(), e);
                }
              });
    }
  }
}
