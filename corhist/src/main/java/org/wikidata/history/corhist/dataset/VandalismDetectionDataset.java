package org.wikidata.history.corhist.dataset;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.wikidata.history.sparql.HistoryRepository;
import org.wikidata.history.sparql.Vocabulary;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class VandalismDetectionDataset {
  private static final int MAX_SIZE = 300000;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final IRI SCHEMA_DESCRIPTION = SimpleValueFactory.getInstance().createIRI("http://schema.org/description");

  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addRequiredOption("f", "truth", true, "Truth file");
    options.addRequiredOption("i", "index", true, "History index");
    options.addRequiredOption("o", "output", true, "Output file");
    CommandLineParser parser = new DefaultParser();
    CommandLine params = parser.parse(options, args);

    List<Long> goodEdits = new ArrayList<>();
    List<Long> badEdits = new ArrayList<>();

    try (
            BufferedReader truthRead = Files.newBufferedReader(Path.of(params.getOptionValue("truth")))
    ) {
      String truthLine;
      while ((truthLine = truthRead.readLine()) != null) {
        String[] parts = truthLine.trim().split(",");
        Long revisionId = Long.parseLong(parts[0]);
        if (parts[1].equals("T")) {
          badEdits.add(revisionId);
        } else if (!parts[2].equals("T")) {
          goodEdits.add(revisionId);
        }
      }
    }

    System.out.println("There are " + goodEdits.size() + " good edits and " + badEdits.size() + " bad edits.");

    goodEdits = (goodEdits.size() > MAX_SIZE) ? goodEdits.subList(0, MAX_SIZE) : goodEdits;
    badEdits = (badEdits.size() > MAX_SIZE) ? badEdits.subList(0, MAX_SIZE) : badEdits;

    List<EditDescription> descriptions = new ArrayList<>();
    try (
            HistoryRepository repository = new HistoryRepository(Paths.get(params.getOptionValue("index")));
            RepositoryConnection connection = repository.getConnection()
    ) {
      for (Long revisionId : badEdits) {
        descriptions.add(describeEdit(connection, revisionId, true));
      }
      for (Long revisionId : goodEdits) {
        descriptions.add(describeEdit(connection, revisionId, false));
      }

      Collections.shuffle(descriptions);
      try (BufferedWriter writer = Files.newBufferedWriter(Path.of(params.getOptionValue("output")))) {
        for (EditDescription description : descriptions) {
          writer.write(OBJECT_MAPPER.writeValueAsString(description));
          writer.write("\n");
        }
      }
    }
  }

  private static EditDescription describeEdit(RepositoryConnection connection, long revisionId, boolean isBad) {
    EditDescription desc = new EditDescription();

    Set<Statement> additions = getTriples(connection, null, null, null, connection.getValueFactory().createIRI(Vocabulary.REVISION_ADDITIONS_NAMESPACE, Long.toString(revisionId)));
    for (Statement addition : additions) {
      desc.additions.add(describeTriple(connection, addition, revisionId));
    }

    Set<Statement> deletions = getTriples(connection, null, null, null, connection.getValueFactory().createIRI(Vocabulary.REVISION_DELETIONS_NAMESPACE, Long.toString(revisionId)));
    for (Statement deletion : deletions) {
      desc.deletions.add(describeTriple(connection, deletion, revisionId));
    }
    desc.isBad = isBad;
    return desc;
  }

  private static DescribedTriple describeTriple(RepositoryConnection connection, Statement statement, long revisionId) {
    Resource subject = statement.getSubject();
    Value object = statement.getObject();

    DescribedTriple desc = new DescribedTriple();
    desc.subject = NTriplesUtil.toNTriplesString(subject);
    if (subject instanceof IRI) {
      desc.subjectDesc = entityDescription(connection, (IRI) subject, revisionId);
    }
    desc.predicate = NTriplesUtil.toNTriplesString(statement.getPredicate());
    desc.object = NTriplesUtil.toNTriplesString(object);
    if (object instanceof IRI) {
      desc.objectDesc = entityDescription(connection, (IRI) object, revisionId);
    }
    return desc;
  }

  private static EntityDescription entityDescription(RepositoryConnection connection, IRI entity, long revisionId) {
    EntityDescription desc = new EntityDescription(entity);
    for (Statement s : getTriples(connection, entity, null, null, connection.getValueFactory().createIRI(Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE, Long.toString(revisionId - 1)))) {
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
    return desc;

  }

  private static Set<Statement> getTriples(RepositoryConnection connection, Resource subject, IRI predicate, Value object, IRI context) {
    Set<Statement> result = new HashSet<>();
    RepositoryResult<Statement> results = connection.getStatements(subject, predicate, object, context);
    while (results.hasNext()) {
      result.add(results.next());
    }
    return result;
  }

  private static class EditDescription {
    public final List<DescribedTriple> additions = new ArrayList<>();
    public final List<DescribedTriple> deletions = new ArrayList<>();
    public boolean isBad;
  }

  private static class DescribedTriple {
    public String subject;
    public EntityDescription subjectDesc;
    public String predicate;
    public String object;
    public EntityDescription objectDesc;
  }
}
