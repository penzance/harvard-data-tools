package edu.harvard.data.generator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.extension.ExtensionSchema;

public class ExtensionSchemaReaderTest {

  private ExtensionSchema read(final String resource) throws IOException, VerificationException {
    return CodeGenerator.readExtensionSchema("test_schemas/" + resource + ".json");
  }

  @Test
  public void emptyObject() throws IOException, VerificationException {
    final DataSchema schema = read("empty_object");
    assertNotNull(schema);
    assertEquals(schema.getTables().size(), 0);
  }

  @Test
  public void emptyTableArray() throws IOException, VerificationException {
    final DataSchema schema = read("empty_table_array");
    assertNotNull(schema);
    assertEquals(schema.getTables().size(), 0);
  }

  @Test
  public void temporaryTable() throws IOException, VerificationException {
    final DataSchema schema = read("temporary_table");
    assertNotNull(schema);
    final DataSchemaTable table = schema.getTableByName("temporary_table");
    assertNotNull(table);
    assertTrue(table.isTemporary());
    assertNotNull(table.getExpirationPhase());
    assertEquals(table.getExpirationPhase().intValue(), 1);
  }

  @DataProvider(name = "invalid_extension_schemas")
  public static Object[][] invalidExtensionSchemas() {
    return new Object[][] { { "no_name_column" }, { "no_type_column" },
      { "no_length_varchar_column" }, { "zero_length_varchar_column" }, { "duplicate_column" },
      { "negative_table_expiration" }, { "large_table_expiration" } };
  }

  @Test(dataProvider = "invalid_extension_schemas", expectedExceptions = VerificationException.class)
  public void invalidExtensionSchema(final String resource)
      throws IOException, VerificationException {
    read(resource);
  }

}
