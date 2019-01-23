package org.wikidata.history.corhist.game;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

final class ViolationDatabase implements AutoCloseable {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(ViolationDatabase.class);
  private static final TypeReference<TreeMap<String, String>> MAP_STR_STR = new TypeReference<TreeMap<String, String>>() {
  };

  private final Connection connection;
  private final PreparedStatement makeObsoleteForEntityPreparedStatement;
  private final PreparedStatement insertionPreparedStatement;
  private final PreparedStatement getViolationStatusStatement;
  private final PreparedStatement updateViolationStatusStatement;
  private final PreparedStatement findViolationsPreparedStatement;
  private final PreparedStatement findConstraintTypesStatement;
  private final PreparedStatement findViolationsForConstraintTypePreparedStatement;
  private final PreparedStatement logActionPreparedStatement;
  private final PreparedStatement countByTypeStateAndUserPreparedStatement;

  ViolationDatabase() throws SQLException {
    connection = DriverManager.getConnection("jdbc:hsqldb:file:violationdb;sql.syntax_mys=true;get_column_name=false", "SA", "");
    setupCorrectionTable();
    makeObsoleteForEntityPreparedStatement = connection.prepareStatement("UPDATE correction SET state = 'o' WHERE entityId = ? AND state = 'p'");
    insertionPreparedStatement = connection.prepareStatement("INSERT INTO correction (entityId, propertyId, statementId, constraintId, constraintType, message, edit, state) VALUES (?, ?, ?, ?, ?, ?, ?, 'p')");
    getViolationStatusStatement = connection.prepareStatement("SELECT state FROM correction WHERE statementId = ? AND constraintId = ?");
    updateViolationStatusStatement = connection.prepareStatement("UPDATE correction SET state = ? WHERE statementId = ? AND constraintId = ?");
    findViolationsPreparedStatement = connection.prepareStatement("SELECT * FROM correction WHERE state = 'p' ORDER BY RAND() LIMIT ?");
    findViolationsForConstraintTypePreparedStatement = connection.prepareStatement("SELECT * FROM correction WHERE state = 'p' AND constraintType = ? ORDER BY RAND() LIMIT ?");
    findConstraintTypesStatement = connection.prepareStatement("SELECT DISTINCT constraintType FROM correction");
    logActionPreparedStatement = connection.prepareStatement("UPDATE correction SET state = ?, user_id = ? WHERE id = ?");
    countByTypeStateAndUserPreparedStatement = connection.prepareStatement("SELECT state, constraintType, user_id, COUNT(id) AS count FROM correction GROUP BY state, constraintType, user_id");
  }

  private void setupCorrectionTable() throws SQLException {
    connection.createStatement().execute("CREATE TABLE IF NOT EXISTS correction " +
            " (id             INT IDENTITY PRIMARY KEY NOT NULL," +
            "  entityId       VARCHAR(16) NOT NULL," +
            "  propertyId     VARCHAR(16) NOT NULL," +
            "  statementId    VARCHAR(64) NOT NULL," +
            "  constraintId   VARCHAR(64) NOT NULL," +
            "  constraintType VARCHAR(16) NOT NULL," +
            "  message        TEXT NOT NULL," +
            "  edit           TEXT NOT NULL," +
            "  state          VARCHAR(1) NOT NULL," +
            "  user_d         VARCHAR(256)," +
            "  UNIQUE (statementId, constraintId)," +
            "  INDEX type_index ON (constraintType, constraintId) " +
            ")");
  }

  synchronized void clearProposedViolations(String entityId) {
    try {
      PreparedStatement preparedStatement = makeObsoleteForEntityPreparedStatement;
      preparedStatement.setString(1, entityId);
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  synchronized void addViolation(String entityId, String propertyId, String statementId, String constraintId, String constraintType, String message, Map<String, String> edit) {
    try {
      Optional<State> currentState = getViolationState(statementId, constraintId);
      //Java 9+: use ifPresentOrElse
      if (currentState.isPresent()) {
        if (currentState.get() == State.OBSOLETE) {
          PreparedStatement preparedUpdateStatement = updateViolationStatusStatement;
          preparedUpdateStatement.setString(1, State.PROPOSED.toString());
          preparedUpdateStatement.setString(2, statementId);
          preparedUpdateStatement.setString(3, constraintId);
          preparedUpdateStatement.executeUpdate();
        }
      } else {
        PreparedStatement preparedInsertStatement = insertionPreparedStatement;
        preparedInsertStatement.setString(1, entityId);
        preparedInsertStatement.setString(2, propertyId);
        preparedInsertStatement.setString(3, statementId);
        preparedInsertStatement.setString(4, constraintId);
        preparedInsertStatement.setString(5, constraintType);
        preparedInsertStatement.setString(6, message);
        preparedInsertStatement.setString(7, OBJECT_MAPPER.writeValueAsString(edit));
        preparedInsertStatement.executeUpdate();
      }
    } catch (SQLException | JsonProcessingException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  private synchronized Optional<State> getViolationState(String statementId, String constraintId) throws SQLException {
    PreparedStatement preparedStatement = getViolationStatusStatement;
    preparedStatement.setString(1, statementId);
    preparedStatement.setString(2, constraintId);
    ResultSet resultSet = preparedStatement.executeQuery();
    if (resultSet.next()) {
      return Optional.of(State.fromString(resultSet.getString("state")));
    }
    return Optional.empty();
  }

  synchronized List<PossibleCorrection> getRandomViolations(int limit) {
    try {
      PreparedStatement preparedStatement = findViolationsPreparedStatement;
      preparedStatement.setInt(1, limit);
      return readPossibleCorrectionsResultSet(preparedStatement.executeQuery());
    } catch (SQLException | IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return Collections.emptyList();
  }

  synchronized List<PossibleCorrection> getRandomViolationsForConstraintType(String constraintType, int limit) {
    try {
      PreparedStatement preparedStatement = findViolationsForConstraintTypePreparedStatement;
      preparedStatement.setString(1, constraintType);
      preparedStatement.setInt(2, limit);
      return readPossibleCorrectionsResultSet(preparedStatement.executeQuery());
    } catch (SQLException | IOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return Collections.emptyList();
  }

  private List<PossibleCorrection> readPossibleCorrectionsResultSet(ResultSet resultSet) throws SQLException, IOException {
    List<PossibleCorrection> results = new ArrayList<>();
    while (resultSet.next()) {
      results.add(new PossibleCorrection(
              resultSet.getInt("id"),
              resultSet.getString("entityId"),
              resultSet.getString("propertyId"),
              resultSet.getString("statementId"),
              resultSet.getString("constraintId"),
              resultSet.getString("constraintType"),
              resultSet.getString("message"),
              OBJECT_MAPPER.readValue(resultSet.getAsciiStream("edit"), MAP_STR_STR)
      ));
    }
    return results;
  }

  synchronized List<String> getConstraintTypes() {
    List<String> results = new ArrayList<>();
    try {
      ResultSet resultSet = findConstraintTypesStatement.executeQuery();
      while (resultSet.next()) {
        results.add(resultSet.getString("constraintType"));
      }
    } catch (SQLException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return results;
  }

  synchronized void logAction(int id, boolean isApproved, String user) {
    try {
      PreparedStatement preparedStatement = logActionPreparedStatement;
      preparedStatement.setString(1, isApproved ? "a" : "r");
      preparedStatement.setString(2, user);
      preparedStatement.setInt(3, id);
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  synchronized Map<String, Map<State, Map<String, Long>>> countByTypeStateAndUser() {
    Map<String, Map<State, Map<String, Long>>> results = new HashMap<>();
    try {
      ResultSet resultSet = countByTypeStateAndUserPreparedStatement.executeQuery();
      while (resultSet.next()) {
        Map<String, Long> byUser = results.computeIfAbsent(resultSet.getString("constraintType"), (k) -> new HashMap<>())
                .computeIfAbsent(State.fromString(resultSet.getString("state")), (k) -> new HashMap<>());
        byUser.put("*", byUser.getOrDefault("*", 0L) + resultSet.getLong("count"));
        if (resultSet.getString("user_id") != null) {
          byUser.put(resultSet.getString("user_id"), resultSet.getLong("count"));
        }
      }
    } catch (SQLException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return results;
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  enum State {
    PROPOSED,
    APPROVED,
    REJECTED,
    OBSOLETE;

    @Override
    public String toString() {
      switch (this) {
        case PROPOSED:
          return "p";
        case APPROVED:
          return "a";
        case REJECTED:
          return "r";
        case OBSOLETE:
          return "o";
        default:
          throw new IllegalArgumentException(this.toString());
      }
    }

    static State fromString(String str) {
      switch (str) {
        case "p":
          return PROPOSED;
        case "a":
          return APPROVED;
        case "r":
          return REJECTED;
        case "o":
          return OBSOLETE;
        default:
          throw new IllegalArgumentException(str);
      }
    }

  }
}
