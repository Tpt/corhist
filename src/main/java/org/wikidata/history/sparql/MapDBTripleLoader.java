package org.wikidata.history.sparql;

import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.GroupSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

final class MapDBTripleLoader implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MapDBTripleLoader.class);

  private final MapDBStore store;
  private final MapDBStore.MapDBStringStore stringStore;
  private final NumericValueFactory valueFactory;

  MapDBTripleLoader(Path path) {
    store = new MapDBStore(path);
    stringStore = store.newInMemoryStringStore();
    valueFactory = new NumericValueFactory(stringStore);
  }

  void load(Path directory) throws IOException {
    try (DB db = DBMaker.memoryDB().make()) {
      BTreeMap<NumericTriple, long[]> spoIndex = newMemoryTreeMap(db, "spo", NumericTriple.SPOSerializer, Serializer.LONG_ARRAY);
      BTreeMap<NumericTriple, long[]> posIndex = newMemoryTreeMap(db, "pos", NumericTriple.POSSerializer, Serializer.LONG_ARRAY);

      LOGGER.info("Loading triples");
      loadDirectStatements(directory.resolve("direct_statements.tsv.gz"), spoIndex, posIndex);
      LOGGER.info("Loading redirections");
      loadRedirections(directory.resolve("redirections.tsv.gz"), spoIndex, posIndex);

      LOGGER.info("Saving string store");
      store.saveStringStore(stringStore);

      try {
        LOGGER.info("Computing P279 closure");
        computeClosure(
                valueFactory.encodePredicate(valueFactory.createIRI(Vocabulary.WDT_NAMESPACE, "P279")),
                valueFactory.encodePredicate(Vocabulary.P279_CLOSURE),
                spoIndex,
                posIndex);

      } catch (NotSupportedValueException e) {
      }

      LOGGER.info("Saving content triples");
      store.spoStatementIndex().buildFrom(spoIndex);
      store.posStatementIndex().buildFrom(posIndex);
      LOGGER.info("Content saving done");
    }
  }

  private BufferedReader gzipReader(Path path) throws IOException {
    return new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(path))));
  }

  private void loadDirectStatements(Path path, BTreeMap<NumericTriple, long[]> spoIndex, BTreeMap<NumericTriple, long[]> posIndex) throws IOException {
    AtomicLong done = new AtomicLong();
    try (BufferedReader reader = gzipReader(path)) {
      reader.lines().parallel().peek(line -> {
        long count = done.getAndIncrement();
        if (count % 1_000_000 == 0) {
          LOGGER.info(count + " triples imported");
        }
      }).forEach(line -> {
        String[] parts = line.split("\t");
        try {
          long[] revisionIds = Arrays.stream(parts[3].split(" ")).mapToLong(Long::parseLong).toArray();
          if (!LongRangeUtils.isSorted(revisionIds)) {
            LOGGER.error("the revision ranges are not sorted: " + Arrays.toString(revisionIds));
          }
          addTriple(spoIndex, posIndex,
                  valueFactory.encodeSubject(NTriplesUtil.parseURI(parts[0], valueFactory)),
                  valueFactory.encodePredicate(NTriplesUtil.parseURI(parts[1], valueFactory)),
                  valueFactory.encodeValue(NTriplesUtil.parseValue(parts[2], valueFactory)),
                  revisionIds
          );
        } catch (NotSupportedValueException e) {
        } catch (Exception e) {
          LOGGER.error(e.getMessage(), e);
        }
      });
    }
  }

  private void loadRedirections(Path path, BTreeMap<NumericTriple, long[]> spoIndex, BTreeMap<NumericTriple, long[]> posIndex) throws IOException {
    try (BufferedReader reader = gzipReader(path)) {
      reader.lines().parallel().forEach(line -> {
        String[] parts = line.split("\t");
        try {
          long[] revisionIds = Arrays.stream(parts[2].split(" ")).mapToLong(Long::parseLong).toArray();
          if (revisionIds.length == 1) {
            revisionIds = new long[]{revisionIds[0], Long.MAX_VALUE};
          }
          addTriple(spoIndex, posIndex,
                  valueFactory.encodeSubject(valueFactory.createIRI(Vocabulary.WD_NAMESPACE, parts[0])),
                  valueFactory.encodePredicate(OWL.SAMEAS),
                  valueFactory.encodeValue(valueFactory.createIRI(Vocabulary.WD_NAMESPACE, parts[1])),
                  revisionIds
          );
        } catch (NotSupportedValueException e) {
        } catch (Exception e) {
          LOGGER.error(e.getMessage(), e);
        }
      });
    }
  }

  private void computeClosure(short property, short targetProperty, BTreeMap<NumericTriple, long[]> spoIndex, BTreeMap<NumericTriple, long[]> posIndex) {
    //We copy everything into the closure
    entryIterator(posIndex, 0, property, 0)
            .forEachRemaining(entry -> addTriple(spoIndex, posIndex,
                    entry.getKey().getSubject(),
                    targetProperty,
                    entry.getKey().getObject(),
                    entry.getValue()));

    //We compute the closure
    entryIterator(posIndex, 0, targetProperty, 0).forEachRemaining(targetEntry -> {
      NumericTriple targetTriple = targetEntry.getKey();
      long[] targetRange = targetEntry.getValue();
      entryIterator(posIndex, 0, targetProperty, targetTriple.getSubject()).forEachRemaining(leftEntry ->
              addTriple(spoIndex, posIndex,
                      leftEntry.getKey().getSubject(),
                      targetProperty,
                      targetTriple.getObject(),
                      LongRangeUtils.intersection(targetRange, leftEntry.getValue())
              )
      );
      valueFactory.toSubject(targetTriple.getObject()).ifPresent(subject ->
              entryIterator(spoIndex, subject, targetProperty, 0).forEachRemaining(rightEntry ->
                      addTriple(spoIndex, posIndex,
                              targetTriple.getSubject(),
                              targetProperty,
                              rightEntry.getKey().getObject(),
                              LongRangeUtils.intersection(targetRange, rightEntry.getValue())
                      )
              )
      );
    });
  }

  private static <K, V> Iterator<Map.Entry<K, V>> entryIterator(BTreeMap<K, V> map, K prefix) {
    return map.entryIterator(prefix, true, map.getKeySerializer().nextValue(prefix), false);
  }

  private static <V> Iterator<Map.Entry<NumericTriple, V>> entryIterator(BTreeMap<NumericTriple, V> map, int subject, short predicate, long object) {
    return entryIterator(map, new NumericTriple(subject, predicate, object));
  }

  private static void addTriple(BTreeMap<NumericTriple, long[]> spoIndex, BTreeMap<NumericTriple, long[]> posIndex, int subject, short predicate, long object, long[] range) {
    if (range == null) {
      return;
    }
    NumericTriple newTriple = new NumericTriple(subject, predicate, object);
    long[] existingRange = spoIndex.get(newTriple);
    if (existingRange != null) {
      range = LongRangeUtils.union(existingRange, range);
    }
    spoIndex.put(newTriple, range);
    posIndex.put(newTriple, range);
  }

  private <K, V> BTreeMap<K, V> newMemoryTreeMap(DB db, String name, GroupSerializer<K> keySerializer, GroupSerializer<V> valueSerializer) {
    return db.treeMap(name).keySerializer(keySerializer).valueSerializer(valueSerializer).createOrOpen();
  }

  @Override
  public void close() {
    valueFactory.close();
    store.close();
  }
}
