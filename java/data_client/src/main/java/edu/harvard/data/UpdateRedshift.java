package edu.harvard.data;

import java.sql.SQLException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.generator.SqlGenerator;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.SchemaComparison;
import edu.harvard.data.schema.redshift.RedshiftSchema;

public class UpdateRedshift {
  private static final Logger log = LogManager.getLogger();
  private final DataSchema expectedSchema;

  public UpdateRedshift(final DataSchema expectedSchema) {
    this.expectedSchema = expectedSchema;
  }

  public void update(final AwsUtils aws, final DataConfig config) throws SQLException {
    log.info("Connecting to Redshift to retrieve schema");
    final RedshiftSchema rs = aws.getRedshiftSchema(config);
    final SchemaComparison diff = new SchemaComparison(expectedSchema, rs);
    final Map<String, DataSchemaTable> additions = diff.getAdditions();
    if (additions.size() > 0) {
      log.info("Updating Redshift schema. Changes: " + diff);
    } else {
      log.info("No changes to be made to Redshift");
    }
    for (final String tableName : additions.keySet()) {
      final DataSchemaTable table = additions.get(tableName);
      if (!table.isTemporary()) {
        if (table.getNewlyGenerated()) {
          final String create = SqlGenerator.generateCreateStatement(table, config.datasetName);
          aws.executeRedshiftQuery(create, config);
        } else {
          for (final DataSchemaColumn column : table.getColumns()) {
            final String alter = SqlGenerator.generateAlterStatement(table, config.datasetName,
                column);
            aws.executeRedshiftQuery(alter, config);
          }
        }
      }
    }
  }
}
