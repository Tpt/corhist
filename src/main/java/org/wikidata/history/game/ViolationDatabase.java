package org.wikidata.history.game;

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
  private final PreparedStatement logActionPreparedStatement;

  ViolationDatabase() throws SQLException {
    connection = DriverManager.getConnection("jdbc:hsqldb:file:violationdb;sql.syntax_mys=true", "SA", "");
    setupCorrectionTable();
    makeObsoleteForEntityPreparedStatement = connection.prepareStatement("UPDATE correction SET state = 'o' WHERE entityId = ? AND state = 'p'");
    insertionPreparedStatement = connection.prepareStatement("INSERT INTO correction (entityId, propertyId, statementId, constraintId, constraintType, message, edit, state) VALUES (?, ?, ?, ?, ?, ?, ?, 'p')");
    getViolationStatusStatement = connection.prepareStatement("SELECT state FROM correction WHERE statementId = ? AND constraintId = ?");
    updateViolationStatusStatement = connection.prepareStatement("UPDATE correction SET state = ? WHERE statementId = ? AND constraintId = ?");
    findViolationsPreparedStatement = connection.prepareStatement("SELECT * FROM correction WHERE state = 'p' ORDER BY RAND() LIMIT ?");
    logActionPreparedStatement = connection.prepareStatement("UPDATE correction SET state = ?, user = ? WHERE id = ?");
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
            "  user           VARCHAR(256)," +
            "  UNIQUE (statementId, constraintId)" +
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

  synchronized List<PossibleCorrection> getViolations(int limit) {
    List<PossibleCorrection> results = new ArrayList<>(limit);
    try {
      PreparedStatement preparedStatement = findViolationsPreparedStatement;
      preparedStatement.setInt(1, limit);
      ResultSet resultSet = preparedStatement.executeQuery();
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
    } catch (SQLException | IOException e) {
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

  synchronized Map<State, Long> countByState() {
    Map<State, Long> results = new HashMap<>();
    try {
      ResultSet resultSet = connection.prepareStatement("SELECT state, COUNT(id) AS count FROM correction GROUP BY state").executeQuery();
      while (resultSet.next()) {
        results.put(State.fromString(resultSet.getString("state")), resultSet.getLong("count"));
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
