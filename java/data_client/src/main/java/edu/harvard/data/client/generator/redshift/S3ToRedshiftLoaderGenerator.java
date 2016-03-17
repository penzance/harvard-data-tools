package edu.harvard.data.client.generator.redshift;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import edu.harvard.data.client.generator.SchemaPhase;
import edu.harvard.data.client.generator.SchemaTransformer;
import edu.harvard.data.client.schema.DataSchemaColumn;
import edu.harvard.data.client.schema.DataSchemaTable;

public class S3ToRedshiftLoaderGenerator {

  private final File dir;
  private final SchemaTransformer schemaVersions;

  public S3ToRedshiftLoaderGenerator(final File dir, final SchemaTransformer schemaVersions) {
    this.dir = dir;
    this.schemaVersions = schemaVersions;
  }

  public void generate() throws IOException {
    final File createTableFile = new File(dir, "s3_to_redshift_loader.sql");

    try (final PrintStream out = new PrintStream(new FileOutputStream(createTableFile))) {
      generateRedshiftLoaderFile(out, schemaVersions.getPhase(2));
    }
  }

  private void generateRedshiftLoaderFile(final PrintStream out, final SchemaPhase phase) {
    outputComments(out, phase.getSchema().getVersion());
    for (final DataSchemaTable table : phase.getSchema().getTables().values()) {
      final String tableName = table.getTableName();
      final String stageTableName = table.getTableName() + "_stage";
      final String joinField = table.getColumns().get(0).getName();
      String columnList = "(";
      for (int i=0; i<table.getColumns().size(); i++) {
        final DataSchemaColumn column = table.getColumns().get(i);
        if (i > 0) {
          columnList += ",";
        }
        columnList += column.getName();
      }
      columnList += ")";

      out.println("------- Table " + tableName + "-------");

      // Create a stage table based on the structure of the real table"
      out.println("DROP TABLE IF EXISTS " + stageTableName + ";");
      out.println("CREATE TABLE " + stageTableName + " (LIKE " + tableName + ");");

      // Copy the final incoming data into final the stage table
      out.println("COPY " + stageTableName + " " + columnList + " FROM '<intermediates3bucketandpath>/" + tableName
          + "/' CREDENTIALS '<awskeyandsecret>' DELIMITER '\\t' TRUNCATECOLUMNS GZIP;");

      // Use an inner join with the staging table to delete the rows from the
      // target table that are being updated.
      // Put the delete and insert operations in a single transaction block so
      // that if there is a problem, everything will be rolled back.
      out.println("BEGIN TRANSACTION;");
      out.println("DELETE FROM " + tableName + " USING " + stageTableName + " WHERE " + tableName
          + "." + joinField + " = " + stageTableName + "." + joinField + ";");

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
  }

  private void outputComments(final PrintStream out, final String version) {
    out.println("-- This file was generated on "
        + new SimpleDateFormat("M-dd-yyyy hh:mm:ss").format(new Date())
        + ". Do not manually edit.");
    out.println("-- This file is based on Version " + version + " of the Canvas Data schema");

    out.println(
        "-- See http://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html");
    out.println("-- for Redshift update strategies.");
    out.println();
    out.println("-- NOTE: Eventually I should just use temporary tables, like this:");
    out.println("-- CREATE TEMPORARY TABLE #requests_stage (LIKE requests);");
    out.println("-- COPY #requests_stage");
    out.println(
        "-- FROM '<intermediates3bucketandpath>/requests/' CREDENTIALS '<awskeyandsecret>' DELIMITER '\t' TRUNCATECOLUMNS;");
    out.println();
    out.println();
  }
}
