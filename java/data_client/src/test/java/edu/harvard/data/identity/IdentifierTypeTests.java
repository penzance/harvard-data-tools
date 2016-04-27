package edu.harvard.data.identity;

import static org.junit.Assert.assertTrue;

import java.util.UUID;
import java.util.regex.Matcher;

import org.junit.Test;

public class IdentifierTypeTests {
  @Test
  public void correctResearchUUID() {
    final String uuid = UUID.randomUUID().toString();
    final Matcher matcher = IdentifierType.ResearchUUID.getPattern().matcher(uuid);
    assertTrue(matcher.matches());
  }
  // Test regexes for each string type
  // Test labels for each type
  // Test classes for each type
  // Test runtime exception for Other label
  // Test runtime exceptions for non-string typed patterns.
}
