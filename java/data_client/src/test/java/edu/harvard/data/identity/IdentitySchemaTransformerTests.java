package edu.harvard.data.identity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import edu.harvard.data.VerificationException;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentitySchemaTransformer;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaType;
import edu.harvard.data.schema.extension.ExtensionSchema;
import edu.harvard.data.schema.extension.ExtensionSchemaColumn;
import edu.harvard.data.schema.extension.ExtensionSchemaTable;
import edu.harvard.data.schema.identity.IdentitySchema;

public class IdentitySchemaTransformerTests {

  private static IdentifierType MAIN_ID = IdentifierType.CanvasID;
  private static DataSchemaType MAIN_ID_TYPE = DataSchemaType.BigInt;

  private static final String TABLE_NAME_1 = "table_1";
  private static final String TABLE_NAME_2 = "table_2";
  private static final String MAIN_ID_COL_1 = "main_id_column_1";
  private static final String MAIN_ID_COL_2 = "main_id_column_2";
  private static final String OTHER_ID_COL = "other_id_column";
  private static final String VALUE_COL = "some_column_name";
  Map<String, ExtensionSchemaTable> tables;
  ExtensionSchema schema;

  @Before
  public void setup() {
    setupSchema();
  }

  private void setupSchema() {
    tables = new HashMap<String, ExtensionSchemaTable>();
    List<DataSchemaColumn> columns = new ArrayList<DataSchemaColumn>();
    addColumn(columns, MAIN_ID_COL_1, MAIN_ID_TYPE, null);
    addColumn(columns, MAIN_ID_COL_2, MAIN_ID_TYPE, null);
    addColumn(columns, OTHER_ID_COL, DataSchemaType.VarChar, 255);
    addColumn(columns, VALUE_COL, DataSchemaType.Boolean, null);
    tables.put(TABLE_NAME_1, new ExtensionSchemaTable(TABLE_NAME_1, columns));

    columns = new ArrayList<DataSchemaColumn>();
    addColumn(columns, MAIN_ID_COL_1, MAIN_ID_TYPE, null);
    addColumn(columns, OTHER_ID_COL, DataSchemaType.VarChar, 255);
    addColumn(columns, VALUE_COL, DataSchemaType.Boolean, null);
    tables.put(TABLE_NAME_2, new ExtensionSchemaTable(TABLE_NAME_2, columns));

    schema = new ExtensionSchema(tables);
  }

  private void verifySchema(final DataSchema s) {
    assertEquals(2, s.getTables().size());
    assertNotNull(s.getTableByName(TABLE_NAME_1));
    List<DataSchemaColumn> columns = s.getTableByName(TABLE_NAME_1).getColumns();
    assertEquals(4, columns.size());
    assertEquals(MAIN_ID_COL_1, columns.get(0).getName());
    assertEquals(MAIN_ID_COL_2, columns.get(1).getName());
    assertEquals(OTHER_ID_COL, columns.get(2).getName());
    assertEquals(VALUE_COL, columns.get(3).getName());

    assertNotNull(s.getTableByName(TABLE_NAME_2));
    columns = s.getTableByName(TABLE_NAME_2).getColumns();
    assertEquals(3, columns.size());
    assertEquals(MAIN_ID_COL_1, columns.get(0).getName());
    assertEquals(OTHER_ID_COL, columns.get(1).getName());
    assertEquals(VALUE_COL, columns.get(2).getName());
  }

  private void addColumn(final List<DataSchemaColumn> cols, final String name,
      final DataSchemaType type, final Integer length) {
    cols.add(new ExtensionSchemaColumn(name, null, type.toString(), length));
  }

  private IdentitySchema getIdMap() {
    return new IdentitySchema(new HashMap<String, Map<String, List<IdentifierType>>>());
  }

  private void addIdentifier(final IdentitySchema idSchema, final String table, final String column,
      final IdentifierType... ids) {
    if (!idSchema.tables.containsKey(table)) {
      idSchema.tables.put(table, new HashMap<String, List<IdentifierType>>());
    }
    if (!idSchema.tables.get(table).containsKey(column)) {
      idSchema.tables.get(table).put(column, new ArrayList<IdentifierType>());
    }
    for (final IdentifierType id : ids) {
      idSchema.tables.get(table).get(column).add(id);
    }
  }

  // Run with empty set of transformations; check new schema matches old.
  @Test
  public void emptyTransformations() throws VerificationException {
    final IdentitySchemaTransformer trans = new IdentitySchemaTransformer(schema, getIdMap(),
        MAIN_ID);
    final DataSchema newSchema = trans.transform();
    verifySchema(newSchema);
  }

  // Run with empty set of transformations; check new schema is a clone
  @Test
  public void cloneSchema() throws VerificationException {
    final IdentitySchemaTransformer trans = new IdentitySchemaTransformer(schema, getIdMap(),
        MAIN_ID);
    final DataSchema newSchema = trans.transform();
    assertFalse(schema == newSchema);
    verifySchema(schema);
  }

  // Check that one main identifier column has been removed.
  @Test
  public void removeOneId() throws VerificationException {
    final IdentitySchema idMap = getIdMap();
    addIdentifier(idMap, TABLE_NAME_1, MAIN_ID_COL_1, MAIN_ID);
    final IdentitySchemaTransformer trans = new IdentitySchemaTransformer(schema, idMap, MAIN_ID);
    final DataSchema newSchema = trans.transform();
    for (final DataSchemaColumn column : newSchema.getTableByName(TABLE_NAME_1).getColumns()) {
      assertNotEquals(MAIN_ID_COL_1, column.getName());
    }
  }

  // Check that one identifier column has been replaced by UUID.
  @Test
  public void addOneUuid() throws VerificationException {
    final IdentitySchema idMap = getIdMap();
    addIdentifier(idMap, TABLE_NAME_1, MAIN_ID_COL_1, MAIN_ID);
    final IdentitySchemaTransformer trans = new IdentitySchemaTransformer(schema, idMap, MAIN_ID);
    final DataSchema newSchema = trans.transform();
    boolean found = false;
    for (final DataSchemaColumn column : newSchema.getTableByName(TABLE_NAME_1).getColumns()) {
      if (column.getName().equals(MAIN_ID_COL_1 + IdentitySchemaTransformer.RESEARCH_UUID_SUFFIX)) {
        found = true;
      }
    }
    assertTrue(found);
  }

  // Check that two main identifier columns have been removed.
  @Test
  public void removeMultipleId() throws VerificationException {
    final IdentitySchema idMap = getIdMap();
    addIdentifier(idMap, TABLE_NAME_1, MAIN_ID_COL_1, MAIN_ID);
    addIdentifier(idMap, TABLE_NAME_1, MAIN_ID_COL_2, MAIN_ID);
    final IdentitySchemaTransformer trans = new IdentitySchemaTransformer(schema, idMap, MAIN_ID);
    final DataSchema newSchema = trans.transform();
    for (final DataSchemaColumn column : newSchema.getTableByName(TABLE_NAME_1).getColumns()) {
      assertNotEquals(MAIN_ID_COL_1, column.getName());
      assertNotEquals(MAIN_ID_COL_2, column.getName());
    }
  }

  // Check that two identifier columns have been replaced by UUIDs.
  @Test
  public void addMultipleUuids() throws VerificationException {
    final IdentitySchema idMap = getIdMap();
    addIdentifier(idMap, TABLE_NAME_1, MAIN_ID_COL_1, MAIN_ID);
    addIdentifier(idMap, TABLE_NAME_1, MAIN_ID_COL_2, MAIN_ID);
    final IdentitySchemaTransformer trans = new IdentitySchemaTransformer(schema, idMap, MAIN_ID);
    final DataSchema newSchema = trans.transform();
    boolean found1 = false;
    boolean found2 = false;
    for (final DataSchemaColumn column : newSchema.getTableByName(TABLE_NAME_1).getColumns()) {
      if (column.getName().equals(MAIN_ID_COL_1 + IdentitySchemaTransformer.RESEARCH_UUID_SUFFIX)) {
        found1 = true;
      }
      if (column.getName().equals(MAIN_ID_COL_2 + IdentitySchemaTransformer.RESEARCH_UUID_SUFFIX)) {
        found2 = true;
      }
    }
    assertTrue(found1);
    assertTrue(found2);
  }

  // Check that an Other identfier column has been removed.
  @Test
  public void removeOtherId() throws VerificationException {
    final IdentitySchema idMap = getIdMap();
    addIdentifier(idMap, TABLE_NAME_1, OTHER_ID_COL, IdentifierType.Other);
    final IdentitySchemaTransformer trans = new IdentitySchemaTransformer(schema, idMap, MAIN_ID);
    final DataSchema newSchema = trans.transform();
    for (final DataSchemaColumn column : newSchema.getTableByName(TABLE_NAME_1).getColumns()) {
      assertNotEquals(OTHER_ID_COL, column.getName());
    }
  }

  // Check that an Other identifier column has not been replaced by UUID.
  @Test
  public void dontAddOtherUuid() throws VerificationException {
    final IdentitySchema idMap = getIdMap();
    addIdentifier(idMap, TABLE_NAME_1, OTHER_ID_COL, IdentifierType.Other);
    final IdentitySchemaTransformer trans = new IdentitySchemaTransformer(schema, idMap, MAIN_ID);
    final DataSchema newSchema = trans.transform();
    boolean found = false;
    for (final DataSchemaColumn column : newSchema.getTableByName(TABLE_NAME_1).getColumns()) {
      if (column.getName().equals(OTHER_ID_COL + IdentitySchemaTransformer.RESEARCH_UUID_SUFFIX)) {
        found = true;
      }
    }
    assertFalse(found);
  }

  // Attempt to transform table that is not in the original schema.
  @Test(expected = VerificationException.class)
  public void transformMissingTable() throws VerificationException {
    final IdentitySchema idMap = getIdMap();
    addIdentifier(idMap, "MissingTable", OTHER_ID_COL, IdentifierType.Other);
    final IdentitySchemaTransformer trans = new IdentitySchemaTransformer(schema, idMap, MAIN_ID);
    trans.transform();
  }

  // Attempt to transform column that is not in the original schema.
  @Test(expected = VerificationException.class)
  public void transformMissingColumn() throws VerificationException {
    final IdentitySchema idMap = getIdMap();
    addIdentifier(idMap, TABLE_NAME_1, "Missing Column", IdentifierType.Other);
    final IdentitySchemaTransformer trans = new IdentitySchemaTransformer(schema, idMap, MAIN_ID);
    trans.transform();
  }
}
