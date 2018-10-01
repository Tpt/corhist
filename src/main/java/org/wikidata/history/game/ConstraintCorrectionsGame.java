package org.wikidata.history.game;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.wdtk.datamodel.helpers.Datamodel;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class ConstraintCorrectionsGame implements Game {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConstraintCorrectionsGame.class);
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  private final ViolationDatabase violationDatabase;
  private final EditDescriber editDescriber = new EditDescriber();

  ConstraintCorrectionsGame(ViolationDatabase violationDatabase) {
    this.violationDatabase = violationDatabase;
  }


  @Override
  public Description getDescription() {
    Description description = new Description();
    description.setLabel("en", "Automated constraint violations corrections");
    description.setDescription("en", "Possible corrections for constraints violations. They are learned from the violations already fixed in the Wikidata edit history. This game have been created by User:Tpt.");

    Option constraintTypeOption = new Option("Constraint type", "constraintType");
    constraintTypeOption.addValue("*", "all constraints");
    for (String constraintType : violationDatabase.getConstraintTypes()) {
      constraintTypeOption.addValue(constraintType, editDescriber.formatValueAsText(Datamodel.makeWikidataItemIdValue(constraintType)));
    }
    description.addOption(constraintTypeOption);

    return description;
  }

  @Override
  public List<Tile> generateTiles(int count, String language, Map<String, String> options) {
    List<PossibleCorrection> corrections = "*".equals(options.getOrDefault("constraintType", "*"))
            ? violationDatabase.getRandomViolations(count)
            : violationDatabase.getRandomViolationsForConstraintType(options.get("constraintType"), count);
    List<Callable<Tile>> tileBuilders = corrections.stream()
            .map(correction -> (Callable<Tile>) () -> buildTile(correction))
            .collect(Collectors.toList());
    try {
      return EXECUTOR.invokeAll(tileBuilders).stream().flatMap(tileFuture -> {
        try {
          return Stream.of(tileFuture.get());
        } catch (InterruptedException | ExecutionException e) {
          LOGGER.error(e.getMessage(), e);
          return Stream.empty();
        }
      }).collect(Collectors.toList());
    } catch (InterruptedException e) {
      LOGGER.error(e.getMessage(), e);
      return Collections.emptyList();
    }
  }

  private Tile buildTile(PossibleCorrection correction) {
    Tile tile = new Tile(correction.getId());

    tile.addSection(new ItemSection(correction.getEntityId()));
    tile.addSection(new TextSection("Violation", correction.getMessage()));
    tile.addSection(new TextSection("Possible correction", editDescriber.toString(correction.getEdit())));
    editDescriber.entities(correction.getEdit())
            .filter(entityId -> !entityId.equals(correction.getEntityId()))
            .forEach(entityId -> tile.addSection(new ItemSection(entityId)));

    tile.addButton(new Button("green", "yes", "Apply the possible correction", correction.getEdit()));
    tile.addButton(new Button("white", "skip", "Skip"));
    tile.addButton(new Button("blue", "no", "The proposed correction is wrong"));

    return tile;
  }

  @Override
  public void log(String user, int tile, String decision) {
    boolean isApproved;
    switch (decision) {
      case "yes":
        isApproved = true;
        break;
      case "no":
        isApproved = false;
        break;
      default:
        LOGGER.error("Unexpected decision from logs: " + decision);
        return;
    }
    violationDatabase.logAction(tile, isApproved, user);
  }
}
