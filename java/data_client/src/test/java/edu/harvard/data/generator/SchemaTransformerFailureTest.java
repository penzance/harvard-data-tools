package edu.harvard.data.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.extension.ExtensionSchema;

@RunWith(Parameterized.class)
public class SchemaTransformerFailureTest {

  private final SchemaTransformer transformer;
  private final String resource1;
  private final String resource2;

  public SchemaTransformerFailureTest(final String resource1, final String resource2) {
    this.transformer = new SchemaTransformer();
    this.resource1 = resource1;
    this.resource2 = resource2;
  }

  private ExtensionSchema read(final String resource) throws IOException, VerificationException {
    return CodeGenerator.readExtensionSchema("test_schemas/" + resource + ".json");
  }

  @Parameters
  public static Collection<Object[]> invalidTransformations() {
    final List<Object[]> data = new ArrayList<Object[]>();
    data.add(new Object[] { "empty_table", "like_fake_table" });
    data.add(new Object[] { "one_column_table", "one_column_table" });
    return data;
  }

  @Test(expected=VerificationException.class)
  public void testInvalidTransformations()
      throws IOException, VerificationException {
    final ExtensionSchema base = read(resource1);
    final ExtensionSchema ext = read(resource2);
    transformer.transform(base, ext);
  }

}
