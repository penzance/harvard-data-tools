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
public class ExtensionSchemaReaderFailureTests {

  private final String resource;

  public ExtensionSchemaReaderFailureTests(final String resource) {
    this.resource = resource;
  }

  private ExtensionSchema read(final String resource) throws IOException, VerificationException {
    return CodeGenerator.readExtensionSchema("test_schemas/" + resource + ".json");
  }

  @Parameters
  public static Collection<Object[]> invalidTransformations() {
    final List<Object[]> data = new ArrayList<Object[]>();
    data.add(new Object[] { "no_name_column" });
    data.add(new Object[] { "no_type_column" });
    data.add(new Object[] { "no_length_varchar_column" });
    data.add(new Object[] { "zero_length_varchar_column" });
    data.add(new Object[] { "duplicate_column" });
    data.add(new Object[] { "negative_table_expiration" });
    data.add(new Object[] { "large_table_expiration" });
    return data;
  }

  @Test(expected=VerificationException.class)
  public void invalidExtensionSchema()
      throws IOException, VerificationException {
    read(resource);
  }

}
