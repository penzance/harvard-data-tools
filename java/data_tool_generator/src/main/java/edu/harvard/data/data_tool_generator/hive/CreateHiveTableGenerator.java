package edu.harvard.data.data_tool_generator.hive;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.client.schema.DataSchemaColumn;
import edu.harvard.data.client.schema.DataSchemaTable;
import edu.harvard.data.data_tool_generator.SchemaPhase;
import edu.harvard.data.data_tool_generator.SchemaTransformer;

public class CreateHiveTableGenerator {

  private static final Logger log = LogManager.getLogger();

  private final SchemaTransformer schemaVersions;
  private final File dir;

  public CreateHiveTableGenerator(final File dir, final SchemaTransformer schemaVersions) {
    this.dir = dir;
    this.schemaVersions = schemaVersions;
  }

  public void generate() throws IOException {
    final File createTableFile = new File(dir, "create_tables.q");

    try (final PrintStream out = new PrintStream(new FileOutputStream(createTableFile))) {
      generateCreateTablesFile(out, schemaVersions);
    }
  }

  // TODO: Generate phase 1 and phase 2 tables separately. Drop the phase 1 tables
  // before creating the phase 2. Maybe that means we can just have in_ and out_ rather
  // than needing the phase_1 or phase_2 prefix?
  private void generateCreateTablesFile(final PrintStream out,
      final SchemaTransformer transformer) {
    log.info("Creating Hive create_tables.q file");

    generateCreateStatements(out, transformer.getPhase(0), "phase_1_in_", true, "SEQUENCEFILE");
    generateCreateStatements(out, transformer.getPhase(1), "phase_1_out_", false, "TEXTFILE");
    //    generateCreateStatements(out, transformer.getPhase(1), "phase_2_in_", true, "TEXTFILE");
    //    generateCreateStatements(out, transformer.getPhase(2), "phase_2_out_", false, "TEXTFILE");
  }

  private void generateCreateStatements(final PrintStream out, final SchemaPhase phaseInput,
      final String prefix, final boolean ignoreOwner, final String format) {
    if (phaseInput != null) {
      final Map<String, DataSchemaTable> inTables = phaseInput.getSchema().getSchema();
      final List<String> inTableKeys = new ArrayList<String>(inTables.keySet());
      Collections.sort(inTableKeys);

      for (final String tableKey : inTableKeys) {
        final DataSchemaTable table = inTables.get(tableKey);
        if (ignoreOwner || table.getOwner() == null || table.getOwner().equals("hive")) {
          final String tableName = prefix + table.getTableName();
          dropTable(out, tableName);
          createTable(out, tableName, table, format, phaseInput.getHDFSDir());
        }
      }
    }
  }

  private void dropTable(final PrintStream out, final String tableName) {
    out.println("DROP TABLE IF EXISTS " + tableName + " PURGE;");
  }

  private void createTable(final PrintStream out, final String tableName,
      final DataSchemaTable table, final String format, final String locationVar) {
    out.println("CREATE EXTERNAL TABLE " + tableName + " (");
    listFields(out, table);
    out.println(")");
    out.println("  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED By '\\n'");
    out.println("  STORED AS " + format);
    out.println("  LOCATION '${" + locationVar + "}/" + table.getTableName() + "/';");
    out.println();
  }

  private void listFields(final PrintStream out, final DataSchemaTable table) {
    final List<DataSchemaColumn> columns = table.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      final DataSchemaColumn column = columns.get(i);
      out.print("    " + column.getName() + " " + column.getHiveType());
      if (i < columns.size() - 1) {
        out.println(",");
      } else {
        out.println();
      }
    }
  }
}
