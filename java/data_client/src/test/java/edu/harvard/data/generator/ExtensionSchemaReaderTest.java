package edu.harvard.data.generator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.extension.ExtensionSchema;

public class ExtensionSchemaReaderTest {

  private ExtensionSchema read(final String resource) throws IOException, VerificationException {
    return ExtensionSchema.readExtensionSchema("test_schemas/" + resource + ".json");
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

}
