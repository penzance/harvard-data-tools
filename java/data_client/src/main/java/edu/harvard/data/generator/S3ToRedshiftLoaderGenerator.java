package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.DataConfig;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentityMap;
import edu.harvard.data.schema.identity.IdentitySchema;
import edu.harvard.data.pipeline.InputTableIndex;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;

public class S3ToRedshiftLoaderGenerator {

  private final File dir;
  private final GenerationSpec spec;
  private final S3ObjectId workingDir;
  private final DataConfig config;
  private final InputTableIndex dataIndex;
  private final IdentitySchema identities;


  public S3ToRedshiftLoaderGenerator(final File dir, final GenerationSpec spec,
      final DataConfig config, final S3ObjectId workingDir, final InputTableIndex dataIndex,
      final IdentitySchema identities) {
    this.config = config;
    this.dir = dir;
    this.spec = spec;
    this.workingDir = workingDir;
    this.dataIndex = dataIndex;
    this.identities = identities;
  }

  public void generate() throws IOException {
    final File createTableFile = new File(dir, spec.getConfig().getRedshiftLoadScript());
    final File identityTableFile = new File(dir, spec.getConfig().getIdentityRedshiftLoadScript());

    try (final PrintStream out = new PrintStream(new FileOutputStream(createTableFile))) {
      generateRedshiftLoaderFile(out, spec.getPhase(3));
    }
    try (final PrintStream out = new PrintStream(new FileOutputStream(identityTableFile))) {
      generateIdentityRedshiftLoaderFile(out, spec.getPhase(3) );
    }
  }

  private void generateIdentityRedshiftLoaderFile(final PrintStream out, final SchemaPhase phase) {
    final Map<String, DataSchemaTable> tables = IdentityMap.getIdentityMapTables(); 
    final List<String> tableNames = new ArrayList<String>();
    for (final DataSchemaTable table : phase.getSchema().getTables().values()) {
      tableNames.add(table.getTableName());
    }
    List<String> checkOptionalIdentityTables = new ArrayList<String>();
    for (final IdentifierType cit : identities.getIdentifierTypes(tableNames) ) {
    	if (!cit.toString().equals("Other")) {
    		checkOptionalIdentityTables.add( IdentifierType.valueOf(cit.toString()).getFieldName());  
    	}
    }
    for (final String tableName : tables.keySet()) {
      if (checkOptionalIdentityTables.contains(tableName) | tableName.equals("identity_map") ) {
		    final DataSchemaTable table = tables.get(tableName);
		    final String columnList = getColumnList(table);
		    final List<String> joinFields = IdentityMap.getPrimaryKeyFields(tableName);
		    outputPartialTableUpdate(out, table, config.getIdentityRedshiftSchema(), columnList,
		        getLocation("identity_map/" + table.getTableName().replaceAll("_", "")), joinFields);  
      }
    }
  }

  private void generateRedshiftLoaderFile(final PrintStream out, final SchemaPhase phase) {
    outputComments(out, phase.getSchema().getVersion());
    final List<String> tableNames = new ArrayList<String>();
    for (final DataSchemaTable table : phase.getSchema().getTables().values()) {
      tableNames.add(table.getTableName());
    }
    Collections.sort(tableNames);
    for (final String tableName : tableNames) {
      if (dataIndex.containsTable(tableName) && (!tableName.equals("assignment_group_fact"))) {
        final DataSchemaTable table = phase.getSchema().getTableByName(tableName);
        final String columnList = getColumnList(table);

        if (!table.isTemporary()) {
          if (dataIndex.isPartial(tableName)) {
            final String joinField = table.getColumns().get(0).getName();
            outputPartialTableUpdate(out, table, config.getDatasetName(), columnList,
                getLocation(table.getTableName()), Collections.singletonList(joinField));
          } else {
            outputTableOverwrite(out, table, config.getDatasetName(), columnList);
          }
        }
      }
    }
  }

  private String getColumnList(final DataSchemaTable table) {
    String columnList = "(";
    for (int i = 0; i < table.getColumns().size(); i++) {
      final DataSchemaColumn column = table.getColumns().get(i);
      String columnName = column.getName();
      if (columnName.contains(".")) {
        columnName = columnName.substring(columnName.lastIndexOf(".") + 1);
      }
      if (i > 0) {
        columnList += ",";
      }
      columnList += columnName;
    }
    columnList += ")";
    return columnList;
  }

  private void outputPartialTableUpdate(final PrintStream out, final DataSchemaTable table,
      final String redshiftSchema, final String columnList, final String s3Location,
      final List<String> joinFields) {
    final String tableName = redshiftSchema + "." + table.getTableName();
    final String stageTableName = redshiftSchema + "_" + table.getTableName() + "_stage";

    out.println("------- Table " + tableName + "-------");
    // Create a stage table based on the structure of the real table"
    out.println("DROP TABLE IF EXISTS " + stageTableName + ";");
    out.println("CREATE TEMPORARY TABLE " + stageTableName + " (LIKE " + tableName + ");");

    // Copy the final incoming data into final the stage table
    out.println("COPY " + stageTableName + " " + columnList + " FROM " + s3Location
        + " CREDENTIALS " + getCredentials() + " DELIMITER '\\t' TRUNCATECOLUMNS GZIP;");

    // Use an inner join with the staging table to delete the rows from the
    // target table that are being updated.
    // Put the delete and insert operations in a single transaction block so
    // that if there is a problem, everything will be rolled back.

    final List<String> joinConditions = new ArrayList<String>();
    for (final String joinField : joinFields) {
      joinConditions.add(tableName + "." + joinField + " = " + stageTableName + "." + joinField);
    }
    final String joinCondition = StringUtils.join(joinConditions, " AND ");

    out.println("BEGIN TRANSACTION;");
    out.println(
        "DELETE FROM " + tableName + " USING " + stageTableName + " WHERE " + joinCondition + ";");

    // Insert all of the rows from the staging table.
    out.println("INSERT INTO " + tableName + " SELECT * FROM " + stageTableName + ";");
    out.println("END TRANSACTION;");

    // Drop the staging table.
    out.println("DROP TABLE " + stageTableName + ";");

    // Vacuum and analyze table for optimal performance.
    out.println("VACUUM " + tableName + ";");
    out.println("ANALYZE " + tableName + ";");
    out.println();
    out.println();
  }

  private void outputTableOverwrite(final PrintStream out, final DataSchemaTable table,
      final String redshiftSchema, final String columnList) {
    final String tableName = redshiftSchema + "." + table.getTableName();

    out.println("------- Table " + tableName + "-------");

    out.println("TRUNCATE " + tableName + ";");
    out.println("VACUUM " + tableName + ";");
    out.println("ANALYZE " + tableName + ";");
    out.println(
        "COPY " + tableName + " " + columnList + " FROM " + getLocation(table.getTableName())
        + " CREDENTIALS " + getCredentials() + " DELIMITER '\\t' TRUNCATECOLUMNS GZIP;");
    out.println("VACUUM " + tableName + ";");
    out.println("ANALYZE " + tableName + ";");
    out.println();
    out.println();
  }

  private void outputComments(final PrintStream out, final String version) {
    out.println("-- This file was automatically generated. Do not manually edit.");

    out.println(
        "-- See http://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html");
    out.println("-- for Redshift update strategies.");
    out.println();
    out.println();
  }

  private String getCredentials() {
    return "'aws_access_key_id=" + spec.getConfig().getAwsKeyId() + ";aws_secret_access_key="
        + spec.getConfig().getAwsSecretKey() + "'";
  }

  private String getLocation(final String tableName) {
    return "'s3://" + workingDir.getBucket() + "/" + workingDir.getKey() + "/"
        + spec.getConfig().getRedshiftStagingDir() + "/" + tableName + "/'";
  }
}
