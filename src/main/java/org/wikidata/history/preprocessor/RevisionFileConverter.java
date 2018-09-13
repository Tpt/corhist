package org.wikidata.history.preprocessor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.wdtk.datamodel.helpers.DataFormatter;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;
import org.wikidata.wdtk.datamodel.helpers.DatamodelMapper;
import org.wikidata.wdtk.datamodel.implementation.EntityDocumentImpl;
import org.wikidata.wdtk.datamodel.interfaces.*;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class RevisionFileConverter {

  private static final Logger LOGGER = LoggerFactory.getLogger("xmlDumpParser");
  private static final Pattern ENTITY_PAGE_TITLE_PATTERN = Pattern.compile("^(Item:|Property:|)([PQ]\\d+)$");
  private static final Pattern REDIRECTION_PATTERN = Pattern.compile("^\\{\"entity\":\"(.*)\",\"redirect\":\"(.*)\"}$");
  private static final String SITE_IRI = "http://www.wikidata.org/entity/";
  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
  private static final long[] EMPTY_LONG_ARRAY = new long[]{};
  private static final DatatypeIdValue EMPTY_DATATYPE = new DatatypeIdValue() {
    @Override
    public String getIri() {
      return "";
    }
  };

  private final ObjectReader entityReader = new DatamodelMapper(SITE_IRI)
          .enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT)
          .readerFor(EntityDocumentImpl.class);
  private final XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();

  private final HistoryOutput historyOutput;

  RevisionFileConverter(HistoryOutput historyOutput) {
    this.historyOutput = historyOutput;
  }

  void process(Path file) {
    try (InputStream fileInputStream = new BZip2CompressorInputStream(Files.newInputStream(file))) {
      processFile(xmlInputFactory.createXMLStreamReader(fileInputStream));
    } catch (IOException | XMLStreamException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  private void processFile(XMLStreamReader reader) throws XMLStreamException, IOException {
    while (reader.hasNext()) {
      switch (reader.getEventType()) {
        case XMLStreamConstants.START_ELEMENT:
          switch (reader.getLocalName()) {
            case "page":
              processPage(reader);
          }
          break;
        case XMLStreamConstants.END_ELEMENT:
          break;
      }
      reader.next();
    }
  }

  private void processPage(XMLStreamReader reader) throws XMLStreamException, IOException {
    String entityId = null;
    List<EntityDocument> revisions = new ArrayList<>();

    reader.next(); //Enter the page
    while (reader.hasNext()) {
      switch (reader.getEventType()) {
        case XMLStreamConstants.START_ELEMENT:
          switch (reader.getLocalName()) {
            case "id":
            case "ns":
              break;
            case "title":
              entityId = getEntityIdFromPageTitle(reader.getElementText());
              break;
            case "redirect":
              break;
            case "revision":
              if (entityId != null) { //Filter out not entities
                processRevision(reader, entityId, revisions);
              }
              break;
          }
          break;
        case XMLStreamConstants.END_ELEMENT:
          if (reader.getLocalName().equals("page")) {
            if (entityId != null) {
              processEntityDocuments(revisions);
            }
            return;
          }
          break;
      }
      reader.next();
    }
  }

  private String getEntityIdFromPageTitle(String title) {
    Matcher matcher = ENTITY_PAGE_TITLE_PATTERN.matcher(title);
    return matcher.matches() ? matcher.group(2) : null;
  }

  private void processRevision(XMLStreamReader reader, String entityId, List<EntityDocument> revisions) throws XMLStreamException, IOException {
    long revisionId = -1;
    long parentRevisionId = -1;
    Instant timestamp = null;
    String contributorName = null;
    String comment = "";
    String sha1 = null;

    reader.next(); //Enter the revision
    while (reader.hasNext()) {
      switch (reader.getEventType()) {
        case XMLStreamConstants.START_ELEMENT:
          switch (reader.getLocalName()) {
            case "comment":
              comment = reader.getElementText();
              break;
            case "contributor":
              contributorName = processContributor(reader);
              break;
            case "format":
              break;
            case "id":
              revisionId = Long.parseLong(reader.getElementText());
              break;
            case "model":
              break;
            case "parentid":
              parentRevisionId = Long.parseLong(reader.getElementText());
              break;
            case "text":
              processRevisionText(reader.getElementText(), revisionId, revisions);
              break;
            case "sha1":
              sha1 = reader.getElementText();
              break;
            case "timestamp":
              timestamp = Instant.parse(reader.getElementText());
              break;
            case "minor":
              break;
            default:
              throw new IllegalArgumentException("Unexpected element \"" + reader.getLocalName() + "\" in revision.");
          }
          break;
        case XMLStreamConstants.END_ELEMENT:
          if (reader.getLocalName().equals("revision")) {
            historyOutput.addRevision(revisionId, parentRevisionId, entityId, timestamp, contributorName, comment, sha1);
            return;
          }
          break;
      }
      reader.next();
    }
  }

  private String processContributor(XMLStreamReader reader) throws XMLStreamException {
    String contributorName = null;

    reader.next(); //Enter the contributor
    while (reader.hasNext()) {
      switch (reader.getEventType()) {
        case XMLStreamConstants.START_ELEMENT:
          switch (reader.getLocalName()) {
            case "id":
              break;
            case "ip":
            case "username":
              contributorName = reader.getElementText();
              break;
            default:
              throw new IllegalArgumentException("Unexpected element \"" + reader.getLocalName() + "\" in contributor.");
          }
          break;
        case XMLStreamConstants.END_ELEMENT:
          if (reader.getLocalName().equals("contributor")) {
            return contributorName;
          }
          break;
      }
      reader.next();
    }

    return contributorName;
  }

  private void processRevisionText(String text, long revisionId, List<EntityDocument> revisions) throws IOException {
    //Redirection
    Matcher redirectionMatcher = REDIRECTION_PATTERN.matcher(text);
    if (redirectionMatcher.matches()) {
      historyOutput.addRedirection(redirectionMatcher.group(1), redirectionMatcher.group(2), revisionId);
      revisions.add(buildEmptyEntityDocument(Datamodel.makeItemIdValue(redirectionMatcher.group(1), SITE_IRI), revisionId));
      return;
    }

    try {
      revisions.add(entityReader.readValue(text));
    } catch (IOException e) {
      LOGGER.warn(e.getMessage(), e);
    }
  }

  private void processEntityDocuments(List<EntityDocument> documents) throws IOException {
    Map<Triple, long[]> triplesHistory = new HashMap<>();

    documents.sort(Comparator.comparingLong(EntityDocument::getRevisionId));

    for (int i = 0; i < documents.size(); i++) {
      EntityDocument document = documents.get(i);
      IRI entityId = VALUE_FACTORY.createIRI(document.getEntityId().getIri());
      long revisionId = document.getRevisionId();
      long nextRevisionId = (i + 1 == documents.size()) ? Long.MAX_VALUE : documents.get(i + 1).getRevisionId();
      if (nextRevisionId < revisionId) {
        LOGGER.error("The revision ids are not properly sorted for entity " + entityId);
      }

      if (document instanceof StatementDocument) {
        for (StatementGroup group : ((StatementDocument) document).getStatementGroups()) {
          IRI propertyId = VALUE_FACTORY.createIRI(group.getProperty().getIri());
          for (Statement statement : getBestStatements(group)) {
            Value value = statement.getValue();
            if (value != null) {
              Triple triple = new Triple(entityId, propertyId, toRDF(value));
              long[] statementRevisions = triplesHistory.getOrDefault(triple, EMPTY_LONG_ARRAY);
              if (statementRevisions.length > 0 && statementRevisions[statementRevisions.length - 1] == revisionId) {
                statementRevisions[statementRevisions.length - 1] = nextRevisionId;
              } else if (statementRevisions.length == 0 || statementRevisions[statementRevisions.length - 1] != nextRevisionId) {
                //We do not add anything if the end is already at nextRevisionId
                statementRevisions = Arrays.copyOf(statementRevisions, statementRevisions.length + 2);
                statementRevisions[statementRevisions.length - 2] = revisionId;
                statementRevisions[statementRevisions.length - 1] = nextRevisionId;
              }
              triplesHistory.put(triple, statementRevisions);
            }
          }
        }
      }
    }

    for (Map.Entry<Triple, long[]> entry : triplesHistory.entrySet()) {
      historyOutput.addDirectStatement(entry.getKey().subject, entry.getKey().predicate, entry.getKey().object, entry.getValue());
    }
  }

  private org.eclipse.rdf4j.model.Value toRDF(Value value) {
    if (value instanceof EntityIdValue) {
      return VALUE_FACTORY.createIRI(((EntityIdValue) value).getIri());
    } else if (value instanceof StringValue) {
      return VALUE_FACTORY.createLiteral(((StringValue) value).getString());
    } else if (value instanceof MonolingualTextValue) {
      MonolingualTextValue text = (MonolingualTextValue) value;
      return VALUE_FACTORY.createLiteral(text.getText(), convertLanguageCode(text.getLanguageCode()));
    } else if (value instanceof TimeValue) {
      TimeValue time = (TimeValue) value;
      return VALUE_FACTORY.createLiteral(DataFormatter.formatTimeISO8601(time), XMLSchema.DATETIME);
    } else if (value instanceof GlobeCoordinatesValue) {
      GlobeCoordinatesValue coords = (GlobeCoordinatesValue) value;
      String wkt = "POINT(" + Double.toString(coords.getLongitude()) + " " + Double.toString(coords.getLatitude()) + ")";
      if (!coords.getGlobe().equals(GlobeCoordinatesValue.GLOBE_EARTH)) {
        wkt += " <" + coords.getGlobe() + ">";
      }
      return VALUE_FACTORY.createLiteral(wkt, GEO.WKT_LITERAL);
    } else if (value instanceof QuantityValue) {
      return VALUE_FACTORY.createLiteral(((QuantityValue) value).getNumericValue());
    } else {
      throw new IllegalArgumentException("Not supported value type: " + value);
    }
  }

  private static String convertLanguageCode(String languageCode) {
    try {
      return WikimediaLanguageCodes.getLanguageCode(languageCode);
    } catch (IllegalArgumentException e) {
      return languageCode;
    }
  }

  private static List<Statement> getBestStatements(StatementGroup statementGroup) {
    List<Statement> preferred = new ArrayList<>();
    List<Statement> normals = new ArrayList<>();
    for (Statement statement : statementGroup.getStatements()) {
      if (statement.getRank().equals(StatementRank.PREFERRED)) {
        preferred.add(statement);
      } else if (statement.getRank().equals(StatementRank.NORMAL)) {
        normals.add(statement);
      }
    }
    return preferred.isEmpty() ? normals : preferred;
  }

  private EntityDocument buildEmptyEntityDocument(EntityIdValue entityId, long revisionId) {
    if (entityId instanceof PropertyIdValue) {
      return Datamodel.makePropertyDocument(
              (PropertyIdValue) entityId,
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              EMPTY_DATATYPE,
              revisionId
      );
    } else if (entityId instanceof ItemIdValue) {
      return Datamodel.makeItemDocument(
              (ItemIdValue) entityId,
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyMap(),
              revisionId
      );
    } else {
      LOGGER.error("Unsupported entity type in redirection: " + entityId);
      return null; //TODO: bad
    }
  }

  private final static class Triple {
    final Resource subject;
    final IRI predicate;
    final org.eclipse.rdf4j.model.Value object;

    Triple(Resource subject, IRI predicate, org.eclipse.rdf4j.model.Value object) {
      this.subject = subject;
      this.predicate = predicate;
      this.object = object;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Triple)) {
        return false;
      }
      Triple t = (Triple) o;
      return subject.equals(t.subject) && predicate.equals(t.predicate) && object.equals(t.object);
    }

    @Override
    public int hashCode() {
      return subject.hashCode() ^ object.hashCode();
    }

    @Override
    public String toString() {
      return NTriplesUtil.toNTriplesString(subject) + ' ' + NTriplesUtil.toNTriplesString(predicate) + ' ' + NTriplesUtil.toNTriplesString(object);
    }
  }
}
