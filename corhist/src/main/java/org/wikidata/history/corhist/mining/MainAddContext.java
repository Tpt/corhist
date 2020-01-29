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
import org.wikidata.history.corhist.dataset.*;
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
  private static final int LIMIT = 200000;
  private static final Logger LOGGER = LoggerFactory.getLogger(MainAddContext.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Map<String, String> URL_FORMATTERS = MainAddContext.fetchUrlFormatters();
  private static final IRI SCHEMA_DESCRIPTION = SimpleValueFactory.getInstance().createIRI("http://schema.org/description");

  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addRequiredOption("f", "file", true, "Constraint file");
    options.addRequiredOption("i", "index", true, "History index");
    options.addRequiredOption("wc", "webCache", true, "Web pages cache");
    options.addRequiredOption("c", "constraints", true, "Constraints file to target");
    CommandLineParser parser = new DefaultParser();
    CommandLine params = parser.parse(options, args);

    List<String> lines;
    try (BufferedReader reader = newReader(Paths.get(params.getOptionValue("file")))) {
      lines = reader.lines().collect(Collectors.toList());
    }
    LOGGER.info("Lines loaded: " + lines.size());

    try (
            WebRetrieval webRetrieval = new WebRetrieval(Paths.get(params.getOptionValue("webCache")));
            HistoryRepository repository = new HistoryRepository(Paths.get(params.getOptionValue("index")));
            RepositoryConnection connection = repository.getConnection();
            BufferedWriter trainOut = newWriter(Paths.get(params.getOptionValue("file") + ".full.train.tsv.gz"));
            BufferedWriter devOut = newWriter(Paths.get(params.getOptionValue("file") + ".full.dev.tsv.gz"));
            BufferedWriter testOut = newWriter(Paths.get(params.getOptionValue("file") + ".full.test.tsv.gz"))
    ) {
      ValueFactory valueFactory = connection.getValueFactory();
      Map<IRI, Constraint> constraints = Constraint.read(Paths.get(params.getOptionValue("constraints")), valueFactory);

      int count = 0;
      for (String line : lines) {
        String[] parts = line.split("\t");
        IRI constraint = NTriplesUtil.parseURI(parts[0], valueFactory);
        IRI context = NTriplesUtil.parseURI(parts[1], valueFactory);
        IRI subject = NTriplesUtil.parseURI(parts[2], valueFactory);
        IRI predicate = NTriplesUtil.parseURI(parts[3], valueFactory);
        Value object = NTriplesUtil.parseValue(parts[4], valueFactory);
        String objectDesc = objectDescription(connection, webRetrieval, object, predicate, context);
        List<Statement> otherTriples = Collections.emptyList();
        try {
          otherTriples = otherTriples(constraint, subject, predicate, object, context, constraints, connection);
        } catch (QueryEvaluationException e) {
          LOGGER.warn(e.getMessage(), e);
        }
        String subjectDesc = entityDescription(connection, subject, context);

        if (otherTriples.isEmpty()) {
          write(Stream.concat(
                  Stream.concat(
                          Arrays.stream(parts).limit(5),
                          Stream.of("", "", "")
                  ),
                  Stream.concat(
                          Arrays.stream(parts).skip(5),
                          Stream.of(subjectDesc, objectDesc, "")
                  )
          ).collect(Collectors.joining("\t")), trainOut, devOut, testOut);
        } else {
          for (Statement otherTriple : otherTriples) {
            String otherDesc = (!subject.equals(otherTriple.getSubject()) && !object.equals(otherTriple.getSubject()))
                    ? entityDescription(connection, (IRI) otherTriple.getSubject(), context)
                    : objectDescription(connection, webRetrieval, otherTriple.getObject(), otherTriple.getPredicate(), context);

            write(Stream.concat(
                    Stream.concat(
                            Arrays.stream(parts).limit(5),
                            Stream.of(otherTriple.getSubject(), otherTriple.getPredicate(), otherTriple.getObject()).map(NTriplesUtil::toNTriplesString)
                    ),
                    Stream.concat(
                            Arrays.stream(parts).skip(5),
                            Stream.of(subjectDesc, objectDesc, otherDesc)
                    )
            ).collect(Collectors.joining("\t")), trainOut, devOut, testOut);
          }
        }
        count += 1;
        if (count > LIMIT) {
          return; // We stop here
        }
      }
    }
  }

  private static void write(String line, BufferedWriter trainOut, BufferedWriter devOut, BufferedWriter testOut) {
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
  }

  private static List<Statement> otherTriples(IRI constraintId, Resource subject, IRI predicate, Value object, IRI context, Map<IRI, Constraint> constraints, RepositoryConnection connection) {
    context = Vocabulary.toGlobalState(Vocabulary.previousRevision(context));

    Constraint constraint = constraints.get(constraintId);
    if (constraint == null) {
      return Collections.emptyList();
    }

    RepositoryResult<Statement> results;
    List<Statement> conflicts = new ArrayList<>();
    switch (constraint.getType().toString()) {
      case "http://www.wikidata.org/entity/Q21502838": // conflict with
        List<Value> propertiesInConflict = constraint.getParameters(QueriesForConstraintCorrectionsBuilder.PROPERTY_PARAMETER);
        if (propertiesInConflict.size() != 1) {
          LOGGER.warn("Invalid constraint: " + constraint.getId());
          break;
        }
        IRI propertyInConflict = Vocabulary.toDirectProperty((IRI) propertiesInConflict.get(0));
        Set<Value> values = new HashSet<>(constraint.getParameters(QueriesForConstraintCorrectionsBuilder.ITEM_PARAMETER));
        results = connection.getStatements(subject, propertyInConflict, null, context);
        results.enableDuplicateFilter();
        while (results.hasNext()) {
          Statement result = results.next();
          if (values.isEmpty() || values.contains(result.getObject())) {
            conflicts.add(result);
          }
        }
        break;
      case "http://www.wikidata.org/entity/Q19474404": // single
        results = connection.getStatements(subject, predicate, null, context);
        results.enableDuplicateFilter();
        while (results.hasNext()) {
          Statement result = results.next();
          if (!object.equals(result.getObject())) {
            conflicts.add(result);
          }
        }
        break;
      case "http://www.wikidata.org/entity/Q21502410": // unique
        results = connection.getStatements(null, predicate, object, context);
        results.enableDuplicateFilter();
        while (results.hasNext()) {
          Statement result = results.next();
          if (!subject.equals(result.getSubject())) {
            conflicts.add(result);
          }
        }
        break;
    }
    return conflicts;
  }

  private static String objectDescription(RepositoryConnection connection, WebRetrieval webRetrieval, Value object, IRI predicate, IRI context) {
    if (URL_FORMATTERS.containsKey(predicate.stringValue()) && !(object instanceof BNode)) {
      String pattern = URL_FORMATTERS.get(predicate.stringValue());
      String url = pattern.equals("$1")
              ? object.stringValue()
              : pattern.replace("$1", wikiUrlEncode(object.stringValue()));
      return pageDescription(webRetrieval, url);
    } else if (object instanceof IRI) {
      return entityDescription(connection, (IRI) object, context);
    } else {
      return "";
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
      LOGGER.warn("Error while retrieving " + entity, e);
      return "";
    }
  }

  private static String pageDescription(WebRetrieval webRetrieval, String url) {
    try {
      WebPage page = webRetrieval.getWebPage(url);
      if (page.getStatusCode() != 200) {
        LOGGER.info(page.getStatusCode() + " status code on page " + page.getLocation());
      }
      return OBJECT_MAPPER.writeValueAsString(page);
    } catch (URISyntaxException e) {
      return "";
    } catch (IOException e) {
      LOGGER.warn("Error while retrieving " + url, e);
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
