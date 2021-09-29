package org.mholford.neo4j;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

public class JourneyProcedure {
  @Context
  public Transaction txn;

  @Context
  public Log log;

  private static String CONDITION_LBL = "Condition";
  private static String ID_PROP = "id";
  private static String NAME_PROP = "name";
  private static String FOUND_CONDITION_REL = "FOUND_CONDITION";
  private static String NEXT_REL = "NEXT";

  @Procedure(name = "org.mholford.neo4j.findJourneys")
  @Description("org.mholford.neo4j.findJourneys(String condition, long numViews)")
  public Stream<JourneyResult> getJourneys(@Name("startCondition") String startCondition,
                                           @Name("numSteps") long numSteps) {

    var startNode = txn.findNode(Label.label(CONDITION_LBL), ID_PROP, startCondition);
    var journeys = getJourneysFrom(startNode, numSteps);
    var journeyResults = collectPaths(journeys);
    return journeyResults.stream();
  }

  /**
   * Get all journeys of length $numSteps, starting from Condition node $n
   * @param n The Condition we are getting journeys for
   * @param numSteps How long each journey must be
   * @return List of journeys (List<String>)
   */
  private List<List<String>> getJourneysFrom(Node n, long numSteps) {
    var journeys = new ArrayList<List<String>>();
    n.getRelationships(Direction.INCOMING, RelationshipType.withName(FOUND_CONDITION_REL)).forEach(r -> {
      var startPoint = r.getOtherNode(n);
      var journey = computeJourney(startPoint, numSteps);
      if (journey != null) {
        journeys.add(journey);
      }
    });
    log.info("Found " + journeys.size() + " journeys");
    return journeys;
  }

  /**
   * Compute one patient's journey starting from the Encounter in which the Condition was found
   * @param n Start Encounter
   * @param numSteps How long the journey must be
   * @return The Journey (List<String>) or null if not enough steps
   */
  private List<String> computeJourney(Node n, long numSteps) {
    var output = new ArrayList<String>();
    var next = n.getSingleRelationship(RelationshipType.withName(NEXT_REL),
            Direction.OUTGOING);
    while (output.size() < numSteps && next != null) {
      n = next.getOtherNode(n);
      var nextConditionRel = n.getSingleRelationship(
              RelationshipType.withName(FOUND_CONDITION_REL), Direction.OUTGOING);
      var nextCondition = nextConditionRel.getOtherNode(n);
      var conditionName = (String) nextCondition.getProperty(NAME_PROP);
      output.add(conditionName);
      next = n.getSingleRelationship(RelationshipType.withName(NEXT_REL),
              Direction.OUTGOING);
    }
    return output.size() == numSteps ? output : null;
  }

  /**
   * Collects common paths with their counts and returns these as JourneyResults
   * @param paths Original (unaggregated) paths
   * @return Journey Results (path + count)
   */
  private List<JourneyResult> collectPaths(List<List<String>> paths) {
    var results = new ArrayList<JourneyResult>();
    var pathMap = new HashMap<List<String>, Integer>();
    for (var path : paths) {
      if (!pathMap.containsKey(path)) {
        pathMap.put(path, 0);
      }
      int newValue = pathMap.get(path) + 1;
      pathMap.put(path, newValue);
    }
    for (var e : pathMap.entrySet()) {
      results.add(new JourneyResult(e.getKey(), e.getValue()));
    }
    return results;
  }
}
