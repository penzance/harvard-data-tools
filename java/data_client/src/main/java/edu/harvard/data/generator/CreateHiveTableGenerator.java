package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
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
    for (int i=1; i<3; i++) {
      final String fileBase = "phase_" + (i+1) + "_create_tables";
      final File phaseFile = new File(dir, fileBase + ".sh");
      try (final PrintStream out = new PrintStream(new FileOutputStream(phaseFile))) {
        log.info("Creating Hive " + phaseFile + " file in " + dir);
        generateCreateTablesFile(out, i + 1, schemaVersions.getPhase(i), schemaVersions.getPhase(i + 1),
            "/home/hadoop/" + fileBase + ".out");
      }
    }
  }

  private void generateCreateTablesFile(final PrintStream out, final int phase, final SchemaPhase input,
      final SchemaPhase output, final String logFile) {
    final List<String> tableNames = new ArrayList<String>(input.getSchema().getTables().keySet());
    Collections.sort(tableNames);
    out.println("sudo mkdir -p /var/log/hive/user/hadoop # Workaround for Hive logging bug");
    out.println("sudo chown hive:hive -R /var/log/hive");
    generateDropStatements(out, phase, "in_", tableNames, input.getSchema().getTables(), logFile );
    out.println();
    generateDropStatements(out, phase, "out_", tableNames, output.getSchema().getTables(), logFile );
    out.println();
    generateCreateStatements(out, phase, input, "in_", true, logFile );
    generateCreateStatements(out, phase, output, "out_", false, logFile );
    out.println("exit $?");
  }

  private void generateDropStatements(final PrintStream out, final int phase, final String prefix,
      final List<String> tableNames, final Map<String, DataSchemaTable> tables, final String logFile ) {
	out.println("sudo hive -e \"");
	for (final String tableName : tableNames) {
      final DataSchemaTable table = tables.get(tableName);
        if (!(table.isTemporary() && table.getExpirationPhase() < phase)) {
          out.println("  DROP TABLE IF EXISTS " + prefix + table.getTableName() + " PURGE;");
        }
    }
	out.println("\" &> " + logFile + "2>&1");
  }

  private void generateCreateStatements(final PrintStream out, final int phase, final SchemaPhase currentPhase,
      final String prefix, final boolean ignoreOwner, final String logFile ) {
    if (currentPhase != null) {
      final Map<String, DataSchemaTable> inTables = currentPhase.getSchema().getTables();
      final List<String> inTableKeys = new ArrayList<String>(inTables.keySet());
      Collections.sort(inTableKeys);

      for (final String tableKey : inTableKeys) {
        final DataSchemaTable table = inTables.get(tableKey);
        if (!(table.isTemporary() && table.getExpirationPhase() < phase)) {
          if (ignoreOwner || (table.getOwner() != null && table.getOwner().equals(TableOwner.hive))) {
            final String tableName = prefix + table.getTableName();
            out.println("if ! hadoop fs -test -e " + currentPhase.getHDFSDir() + "/" + tableName + " ; then ");
            createTable(out, tableName, table, currentPhase.getHDFSDir(), logFile );
            out.println("fi");
            out.println("");
          }
        }
      }
    }
  }

  private void createTable(final PrintStream out, final String tableName,
      final DataSchemaTable table, final String locationVar, final String logFile ) {
	out.println("sudo hive -e \"");
    out.println("  CREATE EXTERNAL TABLE " + tableName + " (");
    listFields(out, table);
    out.println("    )");
    out.println("    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED By '\\n'");
    out.println("    STORED AS TEXTFILE");
    out.println("    LOCATION '" + locationVar + "/" + table.getTableName() + "/';");
    out.println();
    out.println("\" &> " + logFile + "2>&1");
  }

  private void listFields(final PrintStream out, final DataSchemaTable table) {
	final List<String> keywords = Arrays.asList("timestamp", "date");
    final List<DataSchemaColumn> columns = table.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      final DataSchemaColumn column = columns.get(i);
      String columnName = column.getName();
      if (columnName.contains(".")) {
        columnName = columnName.substring(columnName.lastIndexOf(".") + 1);
      }
      if (keywords.contains(columnName)) {
          out.print("    " + "\\`" + columnName + "\\`" + " " + column.getType().getHiveType());
      } else {
          out.print("    " + columnName + " " + column.getType().getHiveType());
      }
      if (i < columns.size() - 1) {
        out.println(",");
      } else {
        out.println();
      }
    }
  }
}
