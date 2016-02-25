package edu.harvard.data.client;

import java.sql.SQLException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.client.generator.redshift.SqlGenerator;
import edu.harvard.data.client.redshift.RedshiftSchema;
import edu.harvard.data.client.schema.DataSchema;
import edu.harvard.data.client.schema.DataSchemaColumn;
import edu.harvard.data.client.schema.DataSchemaTable;
import edu.harvard.data.client.schema.SchemaComparison;

public class UpdateRedshift {
  private static final Logger log = LogManager.getLogger();
  private final DataSchema expectedSchema;

  public UpdateRedshift(final DataSchema expectedSchema) {
    this.expectedSchema = expectedSchema;
  }

  public void update(final AwsUtils aws, final DataConfiguration config) throws SQLException {
    final RedshiftSchema rs = aws.getRedshiftSchema(config);
    final SchemaComparison diff = new SchemaComparison(expectedSchema, rs);
    final Map<String, DataSchemaTable> additions = diff.getAdditions();
    log.info("Updating Redshift schema. Changes: " + diff);
    for (final String tableName : additions.keySet()) {
      final DataSchemaTable table = additions.get(tableName);
      if (table.getNewlyGenerated()) {
        final String create = SqlGenerator.generateCreateStatement(table);
        aws.executeRedshiftQuery(create, config);
      } else {
        for (final DataSchemaColumn column : table.getColumns()) {
          final String alter = SqlGenerator.generateAlterStatement(table, column);
          aws.executeRedshiftQuery(alter, config);
        }
      }
    }

  }
}
