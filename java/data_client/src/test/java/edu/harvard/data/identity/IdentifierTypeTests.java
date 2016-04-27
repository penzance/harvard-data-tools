package edu.harvard.data.identity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

@RunWith(IdentifierTypeTests.class)
@SuiteClasses({ IdentifierTypeTests.CorrectRegexMatches.class,
  IdentifierTypeTests.IncorrectResearchUUID.class, IdentifierTypeTests.IncorrectHUID.class })
public class IdentifierTypeTests extends Suite {

  public IdentifierTypeTests(final Class<?> cls, final RunnerBuilder builder)
      throws InitializationError {
    super(cls, builder);
  }


  public static class CorrectRegexMatches {
    @Test
    public void randomUUID() {
      final String uuid = UUID.randomUUID().toString();
      final Matcher matcher = IdentifierType.ResearchUUID.getPattern().matcher(uuid);
      assertTrue(matcher.matches());
    }

    @Test
    public void uppercaseUUID() {
      final String uuid = UUID.randomUUID().toString().toUpperCase();
      final Matcher matcher = IdentifierType.ResearchUUID.getPattern().matcher(uuid);
      assertTrue(matcher.matches());
    }

    @Test
    public void lowercaseUUID() {
      final String uuid = UUID.randomUUID().toString().toLowerCase();
      final Matcher matcher = IdentifierType.ResearchUUID.getPattern().matcher(uuid);
      assertTrue(matcher.matches());
    }

    @Test
    public void correctHUID() {
      final Matcher matcher = IdentifierType.HUID.getPattern().matcher("99999999");
      assertTrue(matcher.matches());
    }

    @Test
    public void correctXID() {
      final Matcher matcher = IdentifierType.XID.getPattern().matcher("xx99xxxx");
      assertTrue(matcher.matches());
    }

  }

  @RunWith(Parameterized.class)
  public static class IncorrectResearchUUID {
    private final String id;
    public IncorrectResearchUUID(final String id) {
      this.id = id;
    }
    @Parameters
    public static Collection<Object[]> invalidIDs() {
      final List<Object[]> data = new ArrayList<Object[]>();
      data.add(new Object[] { "" });
      data.add(new Object[] { "123" });
      data.add(new Object[] { "99999999" });
      data.add(new Object[] { "xx99xxxx" });
      data.add(new Object[] { "2787a26b-0bc5-4642-94c9-5db91f16b614-1" });
      data.add(new Object[] { "2787a26b-0bc5-3642-94c9-5db91f16b614" });
      data.add(new Object[] { "2787a26b-0bc5-3642-94c9-5dg91f16b614" });
      return data;
    }
    @Test
    public void testInvalidId() {
      final Matcher matcher = IdentifierType.ResearchUUID.getPattern().matcher(id);
      assertFalse(matcher.matches());
    }
  }

  @RunWith(Parameterized.class)
  public static class IncorrectHUID {
    private final String id;
    public IncorrectHUID(final String id) {
      this.id = id;
    }
    @Parameters
    public static Collection<Object[]> invalidIDs() {
      final List<Object[]> data = new ArrayList<Object[]>();
      data.add(new Object[] { "" });
      data.add(new Object[] { "123" });
      data.add(new Object[] { "2787a26b-0bc5-4642-94c9-5db91f16b614" });
      data.add(new Object[] { "xx99xxxx" });
      data.add(new Object[] { "999999999" });
      data.add(new Object[] { "9999999" });
      data.add(new Object[] { "9999999a" });
      return data;
    }
    @Test
    public void testInvalidId() {
      final Matcher matcher = IdentifierType.HUID.getPattern().matcher(id);
      assertFalse(matcher.matches());
    }
  }

  @RunWith(Parameterized.class)
  public static class IncorrectXID {
    private final String id;
    public IncorrectXID(final String id) {
      this.id = id;
    }
    @Parameters
    public static Collection<Object[]> invalidIDs() {
      final List<Object[]> data = new ArrayList<Object[]>();
      data.add(new Object[] { "" });
      data.add(new Object[] { "123" });
      data.add(new Object[] { "2787a26b-0bc5-4642-94c9-5db91f16b614" });
      data.add(new Object[] { "99999999" });
      data.add(new Object[] { "9999999a" });
      return data;
    }
    @Test
    public void testInvalidId() {
      final Matcher matcher = IdentifierType.XID.getPattern().matcher(id);
      assertFalse(matcher.matches());
    }
  }

  // Test labels for each type
  @RunWith(Parameterized.class)
  public static class FieldLabels {
    private final String label;
    private final String constant;
    public FieldLabels(final String constant, final String label) {
      this.constant = constant;
      this.label = label;
    }
    @Parameters
    public static Collection<Object[]> invalidIDs() {
      final List<Object[]> data = new ArrayList<Object[]>();
      data.add(new Object[] { "CanvasDataID", "canvas_data_id" });
      data.add(new Object[] { "CanvasID", "canvas_id" });
      data.add(new Object[] { "HUID", "huid" });
      data.add(new Object[] { "XID", "xid" });
      data.add(new Object[] { "ResearchUUID", "research_id" });
      return data;
    }
    @Test
    public void testFieldLabel() {
      final IdentifierType t = IdentifierType.valueOf(constant);
      assertEquals(label, t.getFieldName());
    }
  }

  // Test classes for each type
  @RunWith(Parameterized.class)
  public static class IdentifierTypes {
    private final Class<?> cls;
    private final String constant;
    public IdentifierTypes(final String constant, final Class<?> cls) {
      this.constant = constant;
      this.cls = cls;
    }
    @Parameters
    public static Collection<Object[]> invalidIDs() {
      final List<Object[]> data = new ArrayList<Object[]>();
      data.add(new Object[] { "CanvasDataID", Long.class });
      data.add(new Object[] { "CanvasID", Long.class });
      data.add(new Object[] { "HUID", String.class });
      data.add(new Object[] { "XID", String.class });
      data.add(new Object[] { "ResearchUUID", String.class });
      data.add(new Object[] { "Other", Void.class });
      return data;
    }
    @Test
    public void testIdentifierType() {
      final IdentifierType t = IdentifierType.valueOf(constant);
      assertEquals(cls, t.getType());
    }
  }

  @RunWith(Parameterized.class)
  public static class InvalidMethodCalls {
    private final String constant;
    public InvalidMethodCalls(final String constant) {
      this.constant = constant;
    }
    @Parameters
    public static Collection<Object[]> invalidIDs() {
      final List<Object[]> data = new ArrayList<Object[]>();
      data.add(new Object[] { "CanvasDataID" });
      data.add(new Object[] { "CanvasID" });
      data.add(new Object[] { "Other" });
      return data;
    }

    // Test runtime exceptions for non-string typed patterns.
    @Test(expected=IdentityImplementationException.class)
    public void testNonStringRegex() {
      final IdentifierType t = IdentifierType.valueOf(constant);
      t.getPattern();
    }

    // Test runtime exception for Other label
    @Test(expected=IdentityImplementationException.class)
    public void otherFieldName() {
      IdentifierType.Other.getFieldName();
    }
  }

}
