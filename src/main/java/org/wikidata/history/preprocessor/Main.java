package org.wikidata.history.preprocessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger("preprocessor");

  public static void main(String[] args) throws IOException, InterruptedException {
    System.setProperty("jdk.xml.entityExpansionLimit", String.valueOf(Integer.MAX_VALUE));
    System.setProperty("jdk.xml.totalEntitySizeLimit", String.valueOf(Integer.MAX_VALUE));


    ExecutorService executorService = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
    );
    try (HistoryOutput historyOutput = new HistoryOutput(Paths.get("out"))) {
      RevisionFileConverter revisionFileConverter = new RevisionFileConverter(historyOutput);
      executorService.invokeAll(
              Files.walk(Paths.get("."))
                      .filter(file -> file.toString().endsWith(".bz2"))
                      .map(file -> (Callable<Void>) () -> {
                        System.out.println(file);
                        try {
                          revisionFileConverter.process(file);
                        } catch (Exception e) {
                          LOGGER.error(e.getMessage(), e);
                        }
                        return null;
                      })
                      .collect(Collectors.toList())
      );
    } finally {
      executorService.shutdown();
    }
  }

}
