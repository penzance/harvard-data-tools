package edu.harvard.data;

import java.sql.SQLException;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.generator.SqlGenerator;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.existing.ExistingSchema;
import edu.harvard.data.schema.existing.ExistingSchemaTable;

public class UnloadExistingTables {
  private static final Logger log = LogManager.getLogger();
  private final ExistingSchema existingSchema;
  private final DataSchema schema;

  public UnloadExistingTables(final ExistingSchema existingSchema, final DataSchema schema) {
    this.existingSchema = existingSchema;
    this.schema = schema;
  }

  public void unload(final AwsUtils aws, final DataConfiguration config, final String s3Location,
      final Date dataBeginDate) throws SQLException {
    log.info("Connecting to Redshift to unload existing tables");
    for (final String tableName : existingSchema.getTables().keySet()) {
      final ExistingSchemaTable table = existingSchema.getTables().get(tableName);
      log.info("Unloading " + tableName);
      final String unload = SqlGenerator.generateUnloadStatement(table,
          schema.getTableByName(tableName), s3Location, config.getAwsKey(),
          config.getAwsSecretKey(), dataBeginDate);
      aws.executeRedshiftQuery(unload, config);
    }
  }
}
