package org.wikidata.history.corhist.mining;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.history.corhist.WikidataSPARQLEndpoint;
import org.wikidata.history.corhist.dataset.EntityDescription;
import org.wikidata.history.corhist.dataset.WebRetrieval;
import org.wikidata.history.sparql.HistoryRepository;
import org.wikidata.history.sparql.Vocabulary;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class MainAddContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(MainAddContext.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Map<String, String> URL_FORMATTERS = MainAddContext.fetchUrlFormatters();
  private static final IRI SCHEMA_DESCRIPTION = SimpleValueFactory.getInstance().createIRI("http://schema.org/description");

  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addRequiredOption("f", "file", true, "Constraint file");
    options.addRequiredOption("i", "index", true, "History index");
    options.addRequiredOption("c", "webCache", true, "Web pages cache");
    CommandLineParser parser = new DefaultParser();
    CommandLine params = parser.parse(options, args);

    List<String> lines;
    try (BufferedReader reader = newReader(Paths.get(params.getOptionValue("file")))) {
      lines = reader.lines().collect(Collectors.toList());
    }
    LOGGER.info("Lines loaded: " + lines.size());

    Collections.shuffle(lines); // In order to avoid doing the HTTP queries site per site

    try (
            WebRetrieval webRetrieval = new WebRetrieval(Paths.get(params.getOptionValue("webCache")));
            HistoryRepository repository = new HistoryRepository(Paths.get(params.getOptionValue("index")));
            RepositoryConnection connection = repository.getConnection();
            BufferedWriter trainOut = newWriter(Paths.get(params.getOptionValue("file") + ".full.train.tsv.gz"));
            BufferedWriter devOut = newWriter(Paths.get(params.getOptionValue("file") + ".full.dev.tsv.gz"));
            BufferedWriter testOut = newWriter(Paths.get(params.getOptionValue("file") + ".full.test.tsv.gz"))
    ) {
      lines.parallelStream().map(line -> {
        ValueFactory valueFactory = connection.getValueFactory();
        String[] parts = line.split("\t");
        IRI context = NTriplesUtil.parseURI(parts[1], valueFactory);
        IRI subject = NTriplesUtil.parseURI(parts[2], valueFactory);
        IRI predicate = NTriplesUtil.parseURI(parts[3], valueFactory);
        Value object = NTriplesUtil.parseValue(parts[4], valueFactory);
        String subjectDesc = entityDescription(connection, subject, context);
        String objectDesc;
        if (URL_FORMATTERS.containsKey(predicate.stringValue()) && !(object instanceof BNode)) {
          String pattern = URL_FORMATTERS.get(predicate.stringValue());
          String url = pattern.equals("$1")
                  ? object.stringValue()
                  : pattern.replace("$1", wikiUrlEncode(object.stringValue()));
          objectDesc = pageDescription(webRetrieval, url);
        } else if (object instanceof IRI) {
          objectDesc = entityDescription(connection, (IRI) object, context);
        } else {
          objectDesc = "";
        }
        return Stream.concat(
                Arrays.stream(parts),
                Stream.of(subjectDesc, objectDesc)
        ).collect(Collectors.joining("\t"));
      }).forEachOrdered(line -> {
        double rand = Math.random();
        Writer writer;
        if (rand < 0.8) {
          writer = trainOut;
        } else if (rand < 0.9) {
          writer = devOut;
        } else {
          writer = testOut;
        }
        try {
          writer.write(line);
          writer.append('\n');
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private static String entityDescription(RepositoryConnection connection, IRI entity, IRI context) {
    try {
      EntityDescription desc = new EntityDescription(entity);
      RepositoryResult<Statement> results = connection.getStatements(entity, null, null, Vocabulary.toGlobalState(Vocabulary.previousRevision(context)));
      results.enableDuplicateFilter();
      while (results.hasNext()) {
        Statement s = results.next();
        IRI predicate = s.getPredicate();
        Value object = s.getObject();
        if (predicate == null || object == null || object.stringValue() == null) {
          //Ignore bad value
        } else if (RDFS.LABEL.equals(predicate)) {
          desc.setLabel(object);
        } else if (SCHEMA_DESCRIPTION.equals(predicate)) {
          desc.setDescription(object);
        } else if (Vocabulary.WDT_NAMESPACE.equals(predicate.getNamespace())) {
          desc.addFact(predicate, object);
        }
      }
      return OBJECT_MAPPER.writeValueAsString(desc);
    } catch (QueryEvaluationException | JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static String pageDescription(WebRetrieval webRetrieval, String url) {
    try {
      return OBJECT_MAPPER.writeValueAsString(webRetrieval.getWebPage(url));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    } catch (URISyntaxException e) {
      return "";
    } catch (InterruptedException | IOException e) {
      LOGGER.warn("Error while getting the URL " + url, e);
      return "";
    }
  }

  private static Map<String, String> fetchUrlFormatters() {
    Map<String, String> result = new HashMap<>();
    try (WikidataSPARQLEndpoint endpoint = new WikidataSPARQLEndpoint()) {
      endpoint.executeTupleQuery("SELECT ?p ?formatter WHERE { _:p wikibase:propertyType wikibase:ExternalId ; wikibase:directClaim ?p ; wdt:P1630 ?formatter }").stream().forEach(bindingSet -> {
        result.put(bindingSet.getValue("p").stringValue(), bindingSet.getValue("formatter").stringValue());
      });
      endpoint.executeTupleQuery("SELECT ?p WHERE { _:p wikibase:propertyType wikibase:Url ; wikibase:directClaim ?p }").stream().forEach(bindingSet -> {
        result.put(bindingSet.getValue("p").stringValue(), "$1");
      });
    }
    LOGGER.info("URL formatters loaded: " + result.size());
    return result;
  }

  private static BufferedReader newReader(Path path) throws IOException {
    if (path.toString().endsWith(".gz")) {
      return new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path))));
    } else {
      return Files.newBufferedReader(path);
    }
  }

  private static BufferedWriter newWriter(Path path) throws IOException {
    if (path.toString().endsWith(".gz")) {
      return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(path))));
    } else {
      return Files.newBufferedWriter(path);
    }
  }

  private static String wikiUrlEncode(String s) {
    s = URLEncoder.encode(s, StandardCharsets.UTF_8);
    for (int i = 0; i < URL_ENCODE_NEEDLE.length; i++) {
      s = s.replace(URL_ENCODE_NEEDLE[i], URL_ENCODE_TARGET[i]);
    }
    return s;
  }

  private static final String[] URL_ENCODE_NEEDLE = {"%3B", "%40", "%24", "%21", "%2A", "%28", "%29", "%2C", "%2F", "%7E", "%3A"};
  private static final String[] URL_ENCODE_TARGET = {";", "@", "$", "!", "*", "(", ")", ",", "/", "~", ":"};
}
