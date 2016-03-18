package edu.harvard.data.generator;

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

import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.TableOwner;

public class CreateHiveTableGenerator {

  private static final Logger log = LogManager.getLogger();

  private final GenerationSpec schemaVersions;
  private final File dir;

  public CreateHiveTableGenerator(final File dir, final GenerationSpec schemaVersions) {
    this.dir = dir;
    this.schemaVersions = schemaVersions;
  }

  public void generate() throws IOException {
    for (int i=0; i<3; i++) {
      final String fileBase = "phase_" + (i+1) + "_create_tables";
      final File phaseFile = new File(dir, fileBase + ".sh");
      try (final PrintStream out = new PrintStream(new FileOutputStream(phaseFile))) {
        log.info("Creating Hive " + phaseFile + " file in " + dir);
        generateCreateTablesFile(out, schemaVersions.getPhase(i), schemaVersions.getPhase(i + 1),
            "/home/hadoop/" + fileBase + ".out");
      }
    }
  }

  private void generateCreateTablesFile(final PrintStream out, final SchemaPhase input,
      final SchemaPhase output, final String logFile) {
    out.println("sudo mkdir -p /var/log/hive/user/hadoop # Workaround for Hive logging bug");
    out.println("sudo chown hive:hive -R /var/log/hive");
    out.println("hive -e \"");
    generateDropStatements(out, "in_", input.getSchema().getTables());
    out.println();
    generateDropStatements(out, "out_", output.getSchema().getTables());
    out.println();
    generateCreateStatements(out, input, "in_", true);
    generateCreateStatements(out, output, "out_", false);
    out.println("\" &> " + logFile);
    out.println("exit $?");
  }

  private void generateDropStatements(final PrintStream out, final String prefix,
      final Map<String, DataSchemaTable> tables) {
    for (final DataSchemaTable table : tables.values()) {
      out.println("  DROP TABLE IF EXISTS " + prefix + table.getTableName() + " PURGE;");
    }
  }

  private void generateCreateStatements(final PrintStream out, final SchemaPhase phase,
      final String prefix, final boolean ignoreOwner) {
    if (phase != null) {
      final Map<String, DataSchemaTable> inTables = phase.getSchema().getTables();
      final List<String> inTableKeys = new ArrayList<String>(inTables.keySet());
      Collections.sort(inTableKeys);

      for (final String tableKey : inTableKeys) {
        final DataSchemaTable table = inTables.get(tableKey);
        if (ignoreOwner || (table.getOwner() != null && table.getOwner().equals(TableOwner.hive))) {
          final String tableName = prefix + table.getTableName();
          createTable(out, tableName, table, phase.getHDFSDir());
        }
      }
    }
  }

  private void createTable(final PrintStream out, final String tableName,
      final DataSchemaTable table, final String locationVar) {
    out.println("  CREATE EXTERNAL TABLE " + tableName + " (");
    listFields(out, table);
    out.println("    )");
    out.println("    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED By '\\n'");
    out.println("    STORED AS TEXTFILE");
    out.println("    LOCATION '" + locationVar + "/" + table.getTableName() + "/';");
    out.println();
  }

  private void listFields(final PrintStream out, final DataSchemaTable table) {
    final List<DataSchemaColumn> columns = table.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      final DataSchemaColumn column = columns.get(i);
      out.print("    " + column.getName() + " " + column.getType().getHiveType());
      if (i < columns.size() - 1) {
        out.println(",");
      } else {
        out.println();
      }
    }
  }
}