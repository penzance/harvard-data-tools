package edu.harvard.data.generator;

import java.io.IOException;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.extension.ExtensionSchema;

public class SchemaTransformerTest {

  private final SchemaTransformer transformer;

  public SchemaTransformerTest() {
    this.transformer = new SchemaTransformer();
  }

  private ExtensionSchema read(final String resource) throws IOException, VerificationException {
    return CodeGenerator.readExtensionSchema("test_schemas/" + resource + ".json");
  }

  @DataProvider(name = "invalid_transformations")
  public static Object[][] invalidTransformations() {
    return new Object[][] { { "empty_table", "like_fake_table" },
      { "one_column_table", "one_column_table" } };
  }

  @Test(dataProvider = "invalid_transformations", expectedExceptions = VerificationException.class)
  public void testInvalidTransformations(final String r1, final String r2)
      throws IOException, VerificationException {
    final ExtensionSchema base = read(r1);
    final ExtensionSchema ext = read(r2);
    transformer.transform(base, ext);
  }

}
