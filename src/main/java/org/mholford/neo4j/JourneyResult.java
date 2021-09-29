package org.mholford.neo4j;

import java.util.List;

/**
 * Representation of a path with the number of times it occurred.
 */
public class JourneyResult {
  public List<String> path;
  public long count;

  public JourneyResult(List<String> path, long count) {
    this.path = path;
    this.count = count;
  }
}
