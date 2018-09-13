package org.wikidata.history.sparql;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.impl.AbstractValueFactory;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class NumericValueFactory extends AbstractValueFactory implements AutoCloseable {

  private static final Pattern TIME_STRING_PATTERN = Pattern.compile("^([+-]?\\d{4,})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2}):(\\d{2})Z$");
  private static final NotSupportedValueException NOT_SUPPORTED_VALUE_EXCEPTION = new NotSupportedValueException();
  private static final Logger LOGGER = LoggerFactory.getLogger(NumericValueFactory.class);

  private static final short HISTORY_P279_CLOSURE_ID = -2;
  private static final short OWL_SAME_AS_ID = -1;
  private static final long TYPE_SHIFT = 8;
  private static final byte ENTITY_ID_TYPE = 0;
  private static final byte STRING_TYPE = 1;
  private static final byte LANGUAGE_STRING_TYPE = 2;
  private static final byte TIME_TYPE = 3;
  private static final byte WKT_TYPE = 4;
  private static final byte DECIMAL_TYPE = 5;
  private static final byte SMALL_LONG_DECIMAL_TYPE = 6;
  private static final byte SMALL_STRING_TYPE = 7;
  private static final long LANGUAGE_TAG_SHIFT = 4096;
  private static final long MAX_ENCODED_VALUE = Long.MAX_VALUE / TYPE_SHIFT;
  private static final long MIN_ENCODED_VALUE = Long.MIN_VALUE / TYPE_SHIFT;

  private static final DatatypeFactory DATATYPE_FACTORY;

  static {
    try {
      DATATYPE_FACTORY = DatatypeFactory.newInstance();
    } catch (DatatypeConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  private final StringStore stringStore;

  NumericValueFactory(StringStore stringStore) {
    this.stringStore = stringStore;
  }

  long encodeValue(Value value) throws NotSupportedValueException {
    if (!(value instanceof NumericValue)) {
      if (value instanceof IRI) {
        value = createIRI((IRI) value);
      } else if (value instanceof BNode) {
        value = createBNode((BNode) value);
      } else if (value instanceof Literal) {
        value = createLiteral((Literal) value);
      }
    }
    if (!(value instanceof NumericValue)) {
      throw NOT_SUPPORTED_VALUE_EXCEPTION;
    }
    return ((NumericValue) value).encode();
  }

  int encodeSubject(Resource subject) throws NotSupportedValueException {
    if (!(subject instanceof NumericSubject) && subject instanceof IRI) {
      subject = createIRI((IRI) subject);
    }
    if (subject instanceof NumericSubject) {
      return ((NumericValueFactory.NumericSubject) subject).encodeSubject();
    }
    throw NOT_SUPPORTED_VALUE_EXCEPTION;
  }

  short encodePredicate(IRI predicate) throws NotSupportedValueException {
    if (predicate.equals(OWL.SAMEAS)) {
      return OWL_SAME_AS_ID;
    }
    if (predicate.equals(Vocabulary.P279_CLOSURE)) {
      return HISTORY_P279_CLOSURE_ID;
    }
    if (!(predicate instanceof NumericPredicate)) {
      predicate = createIRI(predicate);
    }
    if (predicate instanceof NumericPredicate) {
      return ((NumericValueFactory.NumericPredicate) predicate).encodePredicate();
    }
    throw NOT_SUPPORTED_VALUE_EXCEPTION;
  }

  OptionalInt toSubject(long value) {
    byte type = (byte) Math.abs(value % TYPE_SHIFT);
    value /= TYPE_SHIFT;
    switch (type) {
      case ENTITY_ID_TYPE:
        return OptionalInt.of((int) value);
      default:
        return OptionalInt.empty();
    }
  }

  Value createValue(long value) throws NotSupportedValueException {
    byte type = (byte) Math.abs(value % TYPE_SHIFT);
    value /= TYPE_SHIFT;
    switch (type) {
      case ENTITY_ID_TYPE:
        return createEntityIRI((int) value);
      case STRING_TYPE:
        return new DictionaryLiteral(value, XMLSchema.STRING, stringStore);
      case SMALL_STRING_TYPE:
        return new SmallStringLiteral(value);
      case LANGUAGE_STRING_TYPE:
        return new DictionaryLanguageTaggedString(value, stringStore);
      case TIME_TYPE:
        return new TimeLiteral(value);
      case WKT_TYPE:
        return new DictionaryLiteral(value, GEO.WKT_LITERAL, stringStore);
      case DECIMAL_TYPE:
        return new DictionaryLiteral(value, XMLSchema.DECIMAL, stringStore);
      case SMALL_LONG_DECIMAL_TYPE:
        return new SmallLongDecimalLiteral(value);
      default:
        throw NOT_SUPPORTED_VALUE_EXCEPTION;
    }
  }

  @Override
  public IRI createIRI(String iri) {
    int localNameIdx = URIUtil.getLocalNameIndex(iri);
    return createIRI(iri.substring(0, localNameIdx), iri.substring(localNameIdx));
  }

  @Override
  public IRI createIRI(String namespace, String localName) {
    if (Vocabulary.WD_NAMESPACE.equals(namespace)) {
      if (localName.isEmpty()) {
        return super.createIRI(namespace);
      } else if (localName.charAt(0) == 'Q') {
        return new ItemIRI(Integer.parseInt(localName.substring(1)));
      } else if (localName.charAt(0) == 'P') {
        return new PropertyIRI(Short.parseShort(localName.substring(1)), PropertyType.ENTITY);
      } else {
        return super.createIRI(namespace + localName);
      }
    } else if (Vocabulary.WDT_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Short.parseShort(localName.substring(1)), PropertyType.PROP_DIRECT);
    } else if (Vocabulary.P_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Short.parseShort(localName.substring(1)), PropertyType.PROP);
    } else if (Vocabulary.WDNO_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Short.parseShort(localName.substring(1)), PropertyType.PROP_NOVALUE);
    } else if (Vocabulary.PS_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Short.parseShort(localName.substring(1)), PropertyType.PROP_STATEMENT);
    } else if (Vocabulary.PSV_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Short.parseShort(localName.substring(1)), PropertyType.PROP_STATEMENT_VALUE);
    } else if (Vocabulary.PQ_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Short.parseShort(localName.substring(1)), PropertyType.PROP_QUALIFIER);
    } else if (Vocabulary.PQV_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Short.parseShort(localName.substring(1)), PropertyType.PROP_QUALIFIER_VALUE);
    } else if (Vocabulary.PR_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Short.parseShort(localName.substring(1)), PropertyType.PROP_REFERENCE);
    } else if (Vocabulary.PRV_NAMESPACE.equals(namespace)) {
      return new PropertyIRI(Short.parseShort(localName.substring(1)), PropertyType.PROP_REFERENCE_VALUE);
    } else if (Vocabulary.REVISION_NAMESPACE.equals(namespace)) {
      return new RevisionIRI(Long.parseLong(localName), Vocabulary.SnapshotType.NONE);
    } else if (Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE.equals(namespace)) {
      return new RevisionIRI(Long.parseLong(localName), Vocabulary.SnapshotType.GLOBAL_STATE);
    } else if (Vocabulary.REVISION_ADDITIONS_NAMESPACE.equals(namespace)) {
      return new RevisionIRI(Long.parseLong(localName), Vocabulary.SnapshotType.ADDITIONS);
    } else if (Vocabulary.REVISION_DELETIONS_NAMESPACE.equals(namespace)) {
      return new RevisionIRI(Long.parseLong(localName), Vocabulary.SnapshotType.DELETIONS);
    } else {
      return super.createIRI(namespace + localName);
    }
  }

  Value createValue(Value value) {
    if (value instanceof IRI) {
      return createIRI((IRI) value);
    } else if (value instanceof BNode) {
      return createBNode((BNode) value);
    } else if (value instanceof Literal) {
      return createLiteral((Literal) value);
    } else {
      return value;
    }
  }

  Resource createResource(Resource resource) {
    if (resource instanceof IRI) {
      return createIRI((IRI) resource);
    } else if (resource instanceof BNode) {
      return createBNode((BNode) resource);
    } else {
      return resource;
    }
  }

  IRI createIRI(IRI iri) {
    if (iri instanceof NumericValue) {
      return iri;
    }
    return createIRI(iri.getNamespace(), iri.getLocalName());
  }

  BNode createBNode(BNode node) {
    return node;
  }

  @Override
  public Literal createLiteral(String label) {
    if (label.length() <= 7) {
      byte[] encoding = label.getBytes();
      if (encoding.length <= 7) {
        return new SmallStringLiteral(bytesToLong(encoding));
      }
    }
    return createDictionaryLiteral(label, XMLSchema.STRING);
  }

  @Override
  public Literal createLiteral(String label, String language) {
    OptionalLong encodedLabel = stringStore.putString(label);
    Optional<Short> encodedLanguage = stringStore.putLanguage(language);
    if (encodedLabel.isPresent() && encodedLanguage.isPresent()) {
      return new DictionaryLanguageTaggedString(encodedLabel.getAsLong(), encodedLanguage.get(), stringStore);
    } else {
      return super.createLiteral(label, language);
    }
  }

  @Override
  public Literal createLiteral(String label, IRI datatype) {
    if (XMLSchema.STRING.equals(datatype)) {
      return createLiteral(label);
    } else if (XMLSchema.DATETIME.equals(datatype)) {
      return createDateTime(label);
    } else if (GEO.WKT_LITERAL.equals(datatype)) {
      return createDictionaryLiteral(label, datatype);
    } else if (XMLDatatypeUtil.isDecimalDatatype(datatype)) {
      try {
        return createLiteral(Long.parseLong(label));
      } catch (NumberFormatException e) {
        return createDictionaryLiteral(label, datatype);
      }
    } else {
      return super.createLiteral(label, datatype);
    }
  }

  private Literal createDictionaryLiteral(String label, IRI datatype) {
    OptionalLong encodedLabel = stringStore.putString(label);
    if (encodedLabel.isPresent()) {
      return new DictionaryLiteral(encodedLabel.getAsLong(), datatype, stringStore);
    } else {
      return super.createLiteral(label, datatype);
    }
  }

  private static long bytesToLong(byte[] b) {
    long result = 0;
    for (int i = b.length - 1; i >= 0; i--) {
      result <<= 8;
      result |= (b[i] & 0xFF);
    }
    return result;
  }

  private Literal createDateTime(String value) throws IllegalArgumentException {
    Matcher matcher = TIME_STRING_PATTERN.matcher(value);
    if (!matcher.matches()) {
      LOGGER.warn("Not valid time value: " + value);
      return super.createLiteral(value, XMLSchema.DATETIME);
    }
    long year = Long.parseLong(matcher.group(1));
    byte month = Byte.parseByte(matcher.group(2));
    byte day = Byte.parseByte(matcher.group(3));
    byte hours = Byte.parseByte(matcher.group(4));
    byte minutes = Byte.parseByte(matcher.group(5));
    byte seconds = Byte.parseByte(matcher.group(6));
    long encoded = (((((year * 13 + month) * 32 + day) * 25) + hours) * 62 + minutes) * 62 + seconds;
    if (encoded >= MAX_ENCODED_VALUE) {
      LOGGER.warn("Too big time value: " + value);
      return super.createLiteral(value, XMLSchema.DATETIME);
    }
    return new TimeLiteral(encoded);
  }

  @Override
  public Literal createLiteral(byte value) {
    return new SmallLongDecimalLiteral(value);
  }

  @Override
  public Literal createLiteral(short value) {
    return new SmallLongDecimalLiteral(value);
  }

  @Override
  public Literal createLiteral(int value) {
    return new SmallLongDecimalLiteral(value);
  }

  @Override
  public Literal createLiteral(long value) {
    if (MIN_ENCODED_VALUE < value && value < MAX_ENCODED_VALUE) {
      return new SmallLongDecimalLiteral(value);
    } else {
      return createDictionaryLiteral(Long.toString(value), XMLSchema.DECIMAL);
    }
  }

  @Override
  public Literal createLiteral(BigDecimal value) {
    try {
      return createLiteral(value.longValueExact());
    } catch (ArithmeticException e) {
      return createDictionaryLiteral(value.toPlainString(), XMLSchema.DECIMAL);
    }
  }

  @Override
  public Literal createLiteral(BigInteger value) {
    try {
      return createLiteral(value.longValueExact());
    } catch (ArithmeticException e) {
      return createDictionaryLiteral(value.toString(), XMLSchema.DECIMAL);
    }
  }

  Literal createLiteral(Literal literal) {
    if (literal instanceof NumericValue) {
      return literal;
    }
    return literal.getLanguage()
            .map(language -> createLiteral(literal.getLabel(), language))
            .orElseGet(() -> createLiteral(literal.getLabel(), literal.getDatatype()));
  }

  @Override
  public void close() {
    if (stringStore != null) {
      stringStore.close();
    }
  }

  interface NumericValue extends Value {
    long encode();
  }

  interface NumericSubject extends Resource, NumericValue {
    int encodeSubject();
  }

  interface NumericPredicate extends IRI, NumericValue {
    short encodePredicate();
  }

  IRI createEntityIRI(int value) {
    if (value > 0) {
      return new ItemIRI(value);
    } else if (Short.MIN_VALUE <= value && value < 0) {
      return new PropertyIRI((short) -value, PropertyType.ENTITY);
    } else {
      throw new IllegalArgumentException(value + " is not a valid entity revisionId encoding");
    }
  }

  static final class ItemIRI implements IRI, NumericSubject {
    private final int numericId;

    private ItemIRI(int numericId) {
      this.numericId = numericId;
    }

    int getNumericId() {
      return numericId;
    }

    @Override
    public String getNamespace() {
      return Vocabulary.WD_NAMESPACE;
    }

    @Override
    public String getLocalName() {
      return "Q" + Integer.toString(numericId);
    }

    @Override
    public String stringValue() {
      return getNamespace() + getLocalName();
    }

    @Override
    public String toString() {
      return getNamespace() + getLocalName();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof ItemIRI) {
        return ((ItemIRI) o).numericId == numericId;
      } else {
        return o instanceof IRI && o.toString().equals(toString());
      }
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(numericId);
    }

    @Override
    public long encode() {
      return numericId * TYPE_SHIFT + ENTITY_ID_TYPE;
    }

    @Override
    public int encodeSubject() {
      return numericId;
    }
  }

  static final class PropertyIRI implements IRI, NumericSubject, NumericPredicate {
    private final short numericId;
    private final PropertyType propertyType;

    private PropertyIRI(short numericId, PropertyType propertyType) {
      this.numericId = numericId;
      this.propertyType = propertyType;
    }

    short getNumericId() {
      return numericId;
    }

    @Override
    public String getNamespace() {
      switch (propertyType) {
        case ENTITY:
          return Vocabulary.WD_NAMESPACE;
        case PROP_DIRECT:
          return Vocabulary.WDT_NAMESPACE;
        case PROP:
          return Vocabulary.P_NAMESPACE;
        case PROP_NOVALUE:
          return Vocabulary.WDNO_NAMESPACE;
        case PROP_STATEMENT:
          return Vocabulary.PS_NAMESPACE;
        case PROP_STATEMENT_VALUE:
          return Vocabulary.PSV_NAMESPACE;
        case PROP_QUALIFIER:
          return Vocabulary.PQ_NAMESPACE;
        case PROP_QUALIFIER_VALUE:
          return Vocabulary.PQV_NAMESPACE;
        case PROP_REFERENCE:
          return Vocabulary.PR_NAMESPACE;
        case PROP_REFERENCE_VALUE:
          return Vocabulary.PRV_NAMESPACE;
        default:
          throw new IllegalStateException("Not supported property type: " + propertyType);
      }
    }

    @Override
    public String getLocalName() {
      return "P" + Short.toString(numericId);
    }

    @Override
    public String stringValue() {
      return getNamespace() + getLocalName();
    }

    @Override
    public String toString() {
      return getNamespace() + getLocalName();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof PropertyIRI) {
        return ((PropertyIRI) o).numericId == numericId && ((PropertyIRI) o).propertyType == propertyType;
      } else {
        return o instanceof IRI && o.toString().equals(toString());
      }
    }

    @Override
    public int hashCode() {
      return Short.hashCode(numericId);
    }

    @Override
    public long encode() {
      return -numericId * TYPE_SHIFT + ENTITY_ID_TYPE; //TODO: support prefixes
    }

    @Override
    public int encodeSubject() {
      return -numericId; //TODO: support prefixes
    }

    @Override
    public short encodePredicate() {
      return numericId; //TODO: support prefixes
    }
  }

  enum PropertyType {
    ENTITY,
    PROP_DIRECT,
    PROP,
    PROP_NOVALUE,
    PROP_STATEMENT,
    PROP_STATEMENT_VALUE,
    PROP_QUALIFIER,
    PROP_QUALIFIER_VALUE,
    PROP_REFERENCE,
    PROP_REFERENCE_VALUE
  }

  PropertyIRI createDirectPropertyIRI(short value) {
    return new PropertyIRI(value, PropertyType.PROP_DIRECT);
  }

  RevisionIRI createRevisionIRI(long value) {
    return new RevisionIRI(value, Vocabulary.SnapshotType.NONE);
  }

  RevisionIRI createRevisionIRI(long value, Vocabulary.SnapshotType snapshotType) {
    return new RevisionIRI(value, snapshotType);
  }

  static final class RevisionIRI implements IRI {
    private final long revisionId;
    private final Vocabulary.SnapshotType snapshotType;

    private RevisionIRI(long revisionId, Vocabulary.SnapshotType snapshotType) {
      this.revisionId = revisionId;
      this.snapshotType = snapshotType;
    }

    long getRevisionId() {
      return revisionId;
    }

    Vocabulary.SnapshotType getSnapshotType() {
      return snapshotType;
    }

    RevisionIRI withSnapshotType(Vocabulary.SnapshotType snapshotType) {
      return new RevisionIRI(revisionId, snapshotType);
    }

    RevisionIRI previousRevision() {
      return new RevisionIRI(revisionId - 1, snapshotType);
    }

    RevisionIRI nextRevision() {
      return new RevisionIRI(revisionId + 1, snapshotType);
    }

    @Override
    public String getNamespace() {
      switch (snapshotType) {
        case NONE:
          return Vocabulary.REVISION_NAMESPACE;
        case GLOBAL_STATE:
          return Vocabulary.REVISION_GLOBAL_STATE_NAMESPACE;
        case ADDITIONS:
          return Vocabulary.REVISION_ADDITIONS_NAMESPACE;
        case DELETIONS:
          return Vocabulary.REVISION_DELETIONS_NAMESPACE;
        default:
          throw new IllegalArgumentException("Unknown snapshot type:" + snapshotType);
      }
    }

    @Override
    public String getLocalName() {
      return Long.toString(revisionId);
    }

    @Override
    public String stringValue() {
      return getNamespace() + getLocalName();
    }

    @Override
    public String toString() {
      return getNamespace() + getLocalName();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      } else if (o instanceof RevisionIRI) {
        return ((RevisionIRI) o).revisionId == revisionId && ((RevisionIRI) o).snapshotType == snapshotType;
      } else {
        return o instanceof IRI && o.toString().equals(toString());
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(revisionId);
    }
  }

  private static abstract class NumericLiteral implements Literal, NumericValue {
    @Override
    public Optional<String> getLanguage() {
      return Optional.empty();
    }

    @Override
    public boolean booleanValue() {
      return XMLDatatypeUtil.parseBoolean(getLabel());
    }

    @Override
    public byte byteValue() {
      return XMLDatatypeUtil.parseByte(getLabel());
    }

    @Override
    public short shortValue() {
      return XMLDatatypeUtil.parseShort(getLabel());
    }

    @Override
    public int intValue() {
      return XMLDatatypeUtil.parseInt(getLabel());
    }

    @Override
    public long longValue() {
      return XMLDatatypeUtil.parseLong(getLabel());
    }

    @Override
    public float floatValue() {
      return XMLDatatypeUtil.parseFloat(getLabel());
    }

    @Override
    public double doubleValue() {
      return XMLDatatypeUtil.parseDouble(getLabel());
    }

    @Override
    public BigInteger integerValue() {
      return XMLDatatypeUtil.parseInteger(getLabel());
    }

    @Override
    public BigDecimal decimalValue() {
      return XMLDatatypeUtil.parseDecimal(getLabel());
    }

    @Override
    public XMLGregorianCalendar calendarValue() {
      return XMLDatatypeUtil.parseCalendar(getLabel());
    }

    @Override
    public String stringValue() {
      return getLabel();
    }

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    @Override
    public String toString() {
      String label = getLabel();
      IRI datatype = getDatatype();
      StringBuilder sb = new StringBuilder(label.length() + 2);
      sb.append('"').append(label).append('"');
      getLanguage().ifPresent(lang -> sb.append('@').append(lang));
      if (datatype != null && !datatype.equals(XMLSchema.STRING) && !datatype.equals(RDF.LANGSTRING)) {
        sb.append("^^<").append(datatype.toString()).append(">");
      }
      return sb.toString();
    }
  }

  static final class TimeLiteral extends NumericLiteral {

    private final long timestamp;

    private TimeLiteral(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public String getLabel() {
      long value = timestamp;
      long seconds = value % 62;
      value /= 62;
      long minutes = value % 62;
      value /= 62;
      long hours = value % 25;
      value /= 25;
      long day = value % 32;
      value /= 32;
      long month = value % 13;
      long year = value / 13;
      return String.format("%04d-%02d-%02dT%02d:%02d:%02dZ", year, month, day, hours, minutes, seconds);
    }

    @Override
    public IRI getDatatype() {
      return XMLSchema.DATETIME;
    }

    @Override
    public XMLGregorianCalendar calendarValue() {
      long value = timestamp;
      int seconds = (int) (value % 62);
      value /= 62;
      int minutes = (int) (value % 62);
      value /= 62;
      int hours = (int) (value % 25);
      value /= 25;
      int day = (int) (value % 32);
      value /= 32;
      int month = (int) (value % 13);
      BigInteger year = BigInteger.valueOf(value / 13);
      return DATATYPE_FACTORY.newXMLGregorianCalendar(year, month, day, hours, minutes, seconds, BigDecimal.ZERO, 0);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TimeLiteral) {
        return timestamp == ((TimeLiteral) obj).timestamp;
      } else if (obj instanceof Literal) {
        return XMLSchema.DATETIME.equals(((Literal) obj).getDatatype()) && ((Literal) obj).getLabel().equals(getLabel());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(timestamp);
    }

    @Override
    public long encode() {
      return timestamp * TYPE_SHIFT + TIME_TYPE;
    }
  }

  private static final class SmallStringLiteral extends NumericLiteral {
    private long encoding;

    private SmallStringLiteral(long encoding) {
      this.encoding = encoding;
    }

    @Override
    public String getLabel() {
      return new String(longToBytes(encoding));
    }

    private static byte[] longToBytes(long l) {
      //Compute string len
      int len = 0;
      for (long l2 = l; l2 != 0; l2 >>= 8) {
        len++;
      }
      byte[] result = new byte[len];
      for (int i = 0; i < len; i++) {
        result[i] = (byte) (l & 0xFF);
        l >>= 8;
      }
      return result;
    }

    @Override
    public IRI getDatatype() {
      return XMLSchema.STRING;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof SmallStringLiteral) {
        SmallStringLiteral other = (SmallStringLiteral) obj;
        return encoding == other.encoding;
      } else if (obj instanceof Literal) {
        return XMLSchema.STRING.equals(((Literal) obj).getDatatype()) && ((Literal) obj).getLabel().equals(getLabel());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(encoding);
    }

    @Override
    public long encode() {
      return encoding * TYPE_SHIFT + SMALL_STRING_TYPE;
    }
  }

  private static final class DictionaryLiteral extends NumericLiteral {
    private long id;
    private IRI datatype;
    private StringStore stringStore;

    private DictionaryLiteral(long id, IRI datatype, StringStore stringStore) {
      this.id = id;
      this.datatype = datatype;
      this.stringStore = stringStore;
    }

    @Override
    public String getLabel() {
      return stringStore.getString(id).orElseGet(() -> {
        LOGGER.warn("The id " + id + " is not in the string store for datatype: " + datatype);
        return "UNKNOWN";
      });
    }

    @Override
    public IRI getDatatype() {
      return datatype;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DictionaryLiteral) {
        DictionaryLiteral other = (DictionaryLiteral) obj;
        return id == other.id && datatype == other.datatype;
      } else if (obj instanceof Literal) {
        return datatype.equals(((Literal) obj).getDatatype()) && ((Literal) obj).getLabel().equals(getLabel());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(id);
    }

    @Override
    public long encode() {
      if (datatype.equals(XMLSchema.STRING)) {
        return id * TYPE_SHIFT + STRING_TYPE;
      } else if (datatype.equals(GEO.WKT_LITERAL)) {
        return id * TYPE_SHIFT + WKT_TYPE;
      } else if (datatype.equals(XMLSchema.DECIMAL)) {
        return id * TYPE_SHIFT + DECIMAL_TYPE;
      } else {
        throw new IllegalStateException("Not supported datatype for DictionaryLiteral: " + datatype);
      }
    }
  }

  private static final class DictionaryLanguageTaggedString extends NumericLiteral {
    private long labelId;
    private short languageId;
    private StringStore stringStore;

    private DictionaryLanguageTaggedString(long labelId, short languageId, StringStore stringStore) {
      this.labelId = labelId;
      this.languageId = languageId;
      this.stringStore = stringStore;
    }

    private DictionaryLanguageTaggedString(long id, StringStore stringStore) {
      this(id / LANGUAGE_TAG_SHIFT, (short) (id % LANGUAGE_TAG_SHIFT), stringStore);
    }

    @Override
    public String getLabel() {
      return stringStore.getString(labelId).orElseGet(() -> {
        LOGGER.warn("The id " + labelId + " is not in the string store");
        return "UNKNOWN";
      });
    }

    @Override
    public Optional<String> getLanguage() {
      return stringStore.getLanguage(languageId);
    }

    @Override
    public IRI getDatatype() {
      return RDF.LANGSTRING;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof DictionaryLanguageTaggedString) {
        DictionaryLanguageTaggedString other = (DictionaryLanguageTaggedString) obj;
        return labelId == other.labelId && languageId == other.languageId;
      } else if (obj instanceof Literal) {
        return RDF.LANGSTRING.equals(((Literal) obj).getDatatype()) && ((Literal) obj).getLabel().equals(getLabel()) && ((Literal) obj).getLanguage().equals(getLanguage());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(labelId);
    }

    @Override
    public long encode() {
      return ((labelId * LANGUAGE_TAG_SHIFT) + languageId) * TYPE_SHIFT + LANGUAGE_STRING_TYPE;
    }
  }

  private static final class SmallLongDecimalLiteral extends NumericLiteral {
    private long value;

    private SmallLongDecimalLiteral(long value) {
      this.value = value;
    }

    @Override
    public String getLabel() {
      return Long.toString(value);
    }

    @Override
    public IRI getDatatype() {
      return XMLSchema.DECIMAL;
    }

    @Override
    public long longValue() {
      return value;
    }

    @Override
    public BigInteger integerValue() {
      return BigInteger.valueOf(value);
    }

    @Override
    public BigDecimal decimalValue() {
      return BigDecimal.valueOf(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof SmallLongDecimalLiteral) {
        SmallLongDecimalLiteral other = (SmallLongDecimalLiteral) obj;
        return value == other.value;
      } else if (obj instanceof Literal) {
        return XMLSchema.DECIMAL.equals(((Literal) obj).getDatatype()) && ((Literal) obj).getLabel().equals(getLabel());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }

    @Override
    public long encode() {
      return value * TYPE_SHIFT + SMALL_LONG_DECIMAL_TYPE;
    }
  }

  @Override
  public Statement createStatement(Resource subject, IRI predicate, Value object) {
    return super.createStatement(createResource(subject), createIRI(predicate), createValue(object));
  }

  @Override
  public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
    return super.createStatement(createResource(subject), createIRI(predicate), createValue(object), createResource(context));
  }

  interface StringStore extends AutoCloseable {
    Optional<String> getString(long id);

    OptionalLong putString(String str);

    Optional<String> getLanguage(short id);

    Optional<Short> putLanguage(String languageCode);

    @Override
    void close();
  }

  static class EmptyStringStore implements StringStore {
    @Override
    public Optional<String> getString(long id) {
      return Optional.empty();
    }

    @Override
    public OptionalLong putString(String str) {
      return OptionalLong.empty();
    }

    @Override
    public Optional<String> getLanguage(short id) {
      return Optional.empty();
    }

    @Override
    public Optional<Short> putLanguage(String languageCode) {
      return Optional.empty();
    }

    @Override
    public void close() {
    }
  }
}
