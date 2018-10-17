package org.wikidata.history.dataset;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.wikidata.history.IterableTupleQuery;
import org.wikidata.history.dataset.queries.*;
import org.wikidata.history.sparql.Vocabulary;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConstraintViolationCorrectionLookup {

  private static final Map<String, QueriesForConstraintCorrectionsBuilder> SPARQL_BUILDERS = new HashMap<>();

  static {
    SPARQL_BUILDERS.put("unique", new UniqueValueQueriesBuilder());
    SPARQL_BUILDERS.put("inverse", new InverseQueriesBuilder());
    SPARQL_BUILDERS.put("single", new SingleValueQueriesBuilder());
    SPARQL_BUILDERS.put("type", new TypeQueriesBuilder());
    SPARQL_BUILDERS.put("targetClaim", new TargetRequiredClaimQueriesBuilder());
    SPARQL_BUILDERS.put("targetClaimWithValue", new TargetRequiredClaimQueriesBuilder(true));
    SPARQL_BUILDERS.put("valueType", new ValueTypeQueriesBuilder());
    SPARQL_BUILDERS.put("conflict", new ConflictsWithQueriesBuilder());
    SPARQL_BUILDERS.put("oneOf", new OneOfQueriesBuilder());
    SPARQL_BUILDERS.put("item", new ItemQueriesBuilder());
    SPARQL_BUILDERS.put("itemWithValue", new ItemQueriesBuilder(true));
    SPARQL_BUILDERS.put("format", new FormatQueriesBuilder());
  }

  private final List<QueriesForConstraintCorrectionsBuilder> queryBuilders;
  private final Repository repository;
  private final ValueFactory valueFactory;
  private final TupleQuery expandCorrectionFromAdditionQuery;
  private final TupleQuery expandCorrectionFromDeletionQuery;
  private final OptionalLong limit;

  public ConstraintViolationCorrectionLookup(String queryBuilders, Repository repository) {
    this(filterQueriesBuilder(queryBuilders), repository, OptionalLong.empty());
  }

  public ConstraintViolationCorrectionLookup(String queryBuilders, Repository repository, OptionalLong limit) {
    this(filterQueriesBuilder(queryBuilders), repository, limit);
  }

  private ConstraintViolationCorrectionLookup(List<QueriesForConstraintCorrectionsBuilder> queryBuilders, Repository repository, OptionalLong limit) {
    this.queryBuilders = queryBuilders;
    this.repository = repository;
    this.valueFactory = repository.getValueFactory();
    this.expandCorrectionFromAdditionQuery = buildExpandCorrectionFromAdditionQuery();
    this.expandCorrectionFromDeletionQuery = buildExpandCorrectionFromDeletionQuery();
    this.limit = limit;
  }

  private static List<QueriesForConstraintCorrectionsBuilder> filterQueriesBuilder(String selector) {
    if (selector.isEmpty() || selector.equals("*")) {
      return new ArrayList<>(SPARQL_BUILDERS.values());
    } else {
      return Arrays.stream(selector.split(","))
              .peek(name -> {
                if (!SPARQL_BUILDERS.containsKey(name)) {
                  throw new IllegalArgumentException("Not supported conflict type: " + name);
                }
              })
              .map(SPARQL_BUILDERS::get)
              .collect(Collectors.toList());
    }
  }


  public Stream<ConstraintViolationCorrection> findCorrections(Constraint constraint) {
    return findQueries(constraint)
            .flatMap(query -> new IterableTupleQuery(repository, query).stream())
            .map(correction -> buildCorrection(correction, constraint))
            .filter(this::isCorrectionStillApplied);
  }

  private Stream<String> findQueries(Constraint constraint) {
    return queryBuilders.stream()
            .filter(queryBuilder -> queryBuilder.canBuildForConstraint(constraint))
            .flatMap(queryBuilder -> queryBuilder.buildCorrectionsLookupQueries(constraint).stream())
            .map(query -> query + (limit.isPresent() ? " LIMIT " + limit.getAsLong() : ""));
  }

  private ConstraintViolationCorrection buildCorrection(BindingSet bindingSet, Constraint constraint) {
    IRI corrRevision = (IRI) bindingSet.getValue("corrRev");
    Statement target = valueFactory.createStatement(
            (Resource) bindingSet.getValue("targetS"),
            Vocabulary.toDirectProperty(constraint.getProperty()),
            bindingSet.getValue("targetO")
    );
    Statement mainCorrection = valueFactory.createStatement(
            (Resource) bindingSet.getValue("corrS"),
            bindingSet.hasBinding("corrP")
                    ? (IRI) bindingSet.getValue("corrP")
                    : Vocabulary.toDirectProperty(constraint.getProperty()),
            bindingSet.getValue("corrO"),
            bindingSet.getValue("isCorrAddition").equals(BooleanLiteral.TRUE)
                    ? Vocabulary.HISTORY_ADDITION
                    : Vocabulary.HISTORY_DELETION
    );
    Pair<Set<Statement>, IRI> correctionAndRevision = expandCorrection(mainCorrection, target, corrRevision);
    return new ConstraintViolationCorrection(
            constraint,
            target,
            correctionAndRevision.getLeft(),
            correctionAndRevision.getRight()
    );
  }

  private TupleQuery buildExpandCorrectionFromDeletionQuery() {
    return repository.getConnection().prepareTupleQuery("SELECT ?s ?p ?o ?rev (true AS ?isAddition) ?dist WHERE {\n" +
            "    ?mainRev <http://schema.org/author> ?author .\n" +
            "    { BIND(?mainRev AS ?rev). BIND(0 AS ?dist) } UNION { ?rev (<http://schema.org/isBasedOn>|^<http://schema.org/isBasedOn>) ?mainRev . BIND(1 AS ?dist) }\n" +
            "    ?rev <http://schema.org/author> ?author ;\n" +
            "         <http://wikiba.se/history/ontology#additions> ?additions ;\n" +
            "    FILTER NOT EXISTS { GRAPH ?additions { ?targetS ?targetP ?targetO } }\n" +
            "    BIND(?mainS AS ?s)\n" +
            "    GRAPH ?additions {\n" +
            "        {\n" +
            "            BIND(?mainP AS ?p)\n" +
            "            ?s ?p ?o .\n" +
            "        } UNION {\n" +
            "            BIND(?mainO AS ?o)\n" +
            "            ?s ?p ?o .\n" +
            "        }\n" +
            "    }\n" +
            "}");
  }

  private TupleQuery buildExpandCorrectionFromAdditionQuery() {
    return repository.getConnection().prepareTupleQuery("SELECT ?s ?p ?o ?rev (false AS ?isAddition) ?dist WHERE {\n" +
            "    ?mainRev <http://schema.org/author> ?author .\n" +
            "    { BIND(?mainRev AS ?rev) . BIND(0 AS ?dist) } UNION { ?rev (<http://schema.org/isBasedOn>|^<http://schema.org/isBasedOn>) ?mainRev . BIND(1 AS ?dist) }\n" +
            "    ?rev <http://schema.org/author> ?author ;\n" +
            "         <http://wikiba.se/history/ontology#deletions> ?deletions .\n" +
            "    FILTER NOT EXISTS { GRAPH ?additions { ?targetS ?targetP ?targetO } }\n" +
            "    BIND(?mainS AS ?s)\n" +
            "    GRAPH ?deletions {\n" +
            "        {\n" +
            "            BIND(?mainP AS ?p)\n" +
            "            ?s ?p ?o .\n" +
            "        } UNION {\n" +
            "            BIND(?mainO AS ?o)\n" +
            "            ?s ?p ?o .\n" +
            "        }\n" +
            "    }\n" +
            "}");
  }

  private synchronized Pair<Set<Statement>, IRI> expandCorrection(Statement mainCorrection, Statement target, IRI mainRevision) {
    TupleQuery query = mainCorrection.getContext().equals(Vocabulary.HISTORY_DELETION)
            ? expandCorrectionFromDeletionQuery
            : expandCorrectionFromAdditionQuery;
    query.setBinding("mainS", mainCorrection.getSubject());
    query.setBinding("mainP", mainCorrection.getPredicate());
    query.setBinding("mainO", mainCorrection.getObject());
    query.setBinding("targetS", target.getSubject());
    query.setBinding("targetP", target.getPredicate());
    query.setBinding("targetO", target.getObject());
    query.setBinding("mainRev", mainRevision);

    int additionalTripleDistance = Integer.MAX_VALUE;
    long additionalRevisionId = Long.MAX_VALUE;
    Set<Statement> additionalTriples = new HashSet<>();

    for (BindingSet bindingSet : new IterableTupleQuery(query)) {
      int currentDistance = ((Literal) bindingSet.getValue("dist")).intValue();
      if (currentDistance < additionalTripleDistance) {
        additionalTriples = new HashSet<>();
        additionalTripleDistance = currentDistance;
        additionalRevisionId = Long.MAX_VALUE;
      }
      if (currentDistance == additionalTripleDistance) {
        additionalRevisionId = Math.min(additionalRevisionId, Long.parseLong(((IRI) bindingSet.getValue("rev")).getLocalName()));
        additionalTriples.add(valueFactory.createStatement(
                (Resource) bindingSet.getValue("s"),
                (IRI) bindingSet.getValue("p"),
                bindingSet.getValue("o"),
                bindingSet.getValue("isAddition").equals(BooleanLiteral.TRUE)
                        ? Vocabulary.HISTORY_ADDITION
                        : Vocabulary.HISTORY_DELETION
        ));
      }
    }
    query.clearBindings();

    Set<Statement> correction = (additionalTriples.size() > 1) ? new HashSet<>() : additionalTriples;
    correction.add(mainCorrection);
    long revisionId = Math.min(Long.parseLong(mainRevision.getLocalName()), additionalRevisionId);
    return Pair.of(correction, valueFactory.createIRI(Vocabulary.REVISION_NAMESPACE, Long.toString(revisionId)));
  }

  private boolean isCorrectionStillApplied(ConstraintViolationCorrection correction) {
    try (RepositoryConnection connection = repository.getConnection()) {
      return correction.getCorrection().stream().allMatch(statement -> {
        if (statement.getContext().equals(Vocabulary.HISTORY_ADDITION)) {
          return connection.hasStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(), false, Vocabulary.CURRENT_GLOBAL_STATE);
        } else if (statement.getContext().equals(Vocabulary.HISTORY_DELETION)) {
          return !connection.hasStatement(statement.getSubject(), statement.getPredicate(), statement.getObject(), false, Vocabulary.CURRENT_GLOBAL_STATE);
        } else {
          throw new IllegalArgumentException("Not expected correction quad: " + statement);
        }
      });
    }
  }

  public long countCurrentViolations(Constraint constraint) {
    return findCurrentViolationQuery(constraint)
            .map(query -> new IterableTupleQuery(repository, query).stream().count())
            .orElse(0L);
  }

  private Optional<String> findCurrentViolationQuery(Constraint constraint) {
    return queryBuilders.stream()
            .filter(queryBuilder -> queryBuilder.canBuildForConstraint(constraint))
            .map(queryBuilder -> queryBuilder.buildViolationQuery(constraint, Vocabulary.CURRENT_GLOBAL_STATE))
            .findAny();
  }

  public long countCurrentInstances(Constraint constraint) {
    if (!hasQueryBuilder(constraint)) {
      return 0;
    }
    return new IterableTupleQuery(repository, "SELECT (COUNT(*) AS ?c) FROM <" + Vocabulary.CURRENT_GLOBAL_STATE + "> WHERE { ?s <" + Vocabulary.toDirectProperty(constraint.getProperty()) + "> ?o . }").stream()
            .findAny()
            .map(b -> ((Literal) b.getValue("c")).longValue())
            .orElse(0L);
  }

  private boolean hasQueryBuilder(Constraint constraint) {
    return queryBuilders.stream().anyMatch(queryBuilder -> queryBuilder.canBuildForConstraint(constraint));
  }
}
