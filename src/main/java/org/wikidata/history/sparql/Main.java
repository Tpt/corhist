package org.wikidata.history.sparql;

import org.apache.commons.cli.*;
import org.eclipse.rdf4j.query.resultio.text.tsv.SPARQLResultsTSVWriterFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Main {
  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addOption("l", "load", true, "Directory to load data from. Useful to build the indexes");
    options.addOption("t", "triplesOnly", false, "Load only triples");
    options.addOption("q", "sparql", true, "SPARQL query to execute");

    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    Path index = Paths.get("wd-history-index");
    if (line.hasOption("load")) {
      if (!line.hasOption("triplesOnly")) {
        try (MapDBRevisionLoader loader = new MapDBRevisionLoader(index)) {
          loader.load(Paths.get(line.getOptionValue("load")));
        }
      }
      try (MapDBTripleLoader loader = new MapDBTripleLoader(index)) {
        loader.load(Paths.get(line.getOptionValue("load")));
      }
    }
    if (line.hasOption("sparql")) {
      try (HistoryRepository historyRepository = new HistoryRepository(index)) {
        historyRepository.getConnection().prepareTupleQuery(line.getOptionValue("sparql"))
                .evaluate((new SPARQLResultsTSVWriterFactory()).getWriter(System.out));
      }
    }
  }
}
