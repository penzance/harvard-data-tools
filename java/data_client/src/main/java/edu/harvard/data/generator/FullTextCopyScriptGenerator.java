package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.pipeline.InputTableIndex;
import edu.harvard.data.schema.fulltext.FullTextSchema;
import edu.harvard.data.schema.fulltext.FullTextTable;

public class FullTextCopyScriptGenerator {

  private final File dir;
  private final InputTableIndex dataIndex;
  private final DataConfig config;
  private final FullTextSchema textSchema;

  public FullTextCopyScriptGenerator(final File dir, final DataConfig config,
      final FullTextSchema textSchema, final InputTableIndex dataIndex) {
    this.config = config;
    this.dir = dir;
    this.textSchema = textSchema;
    this.dataIndex = dataIndex;
  }

  public void generate() throws IOException {
    final File scriptFile = new File(dir, config.getFullTextScriptFile());

    try (final PrintStream out = new PrintStream(new FileOutputStream(scriptFile))) {
      boolean data = false;
      for (final String table : textSchema.tableNames()) {
        if (dataIndex.containsTable(table)) {
          generateTable(out, table);
          data = true;
        }
      }
      if (data) {
        out.println("aws s3 cp --recursive " + config.getFullTextDir() + "/ "
            + AwsUtils.uri(config.getFullTextLocation()));
      }
    }
  }

  private void generateTable(final PrintStream out, final String tableName) {
    final FullTextTable table = textSchema.get(tableName);
    out.println("mkdir -p /home/hadoop/full_text/" + tableName);
    for (final String column : table.getColumns()) {
      final String filename = config.getFullTextDir() + "/" + tableName + "/" + column;
      out.println("sudo hive -S -e \"select " + table.getKey() + ", " + column + " from in_"
          + tableName + ";\" > " + filename);
      out.println("gzip " + filename);
    }
    if ( dataIndex.isPartial(tableName) ) {
      generateFullTable( out, tableName );
    }
    out.println();
  }
	  
  private void generateFullTable( final PrintStream out, final String tableName) {
    final FullTextTable table = textSchema.get(tableName);
    final String filename = config.getFullTextDir() + "/" + tableName + "/fulltable/" + tableName;
    out.print("sudo hive -S -e \"select " + table.getKey() );
    for (final String column : table.getColumns() ) {
        out.print("," + column);
    }
    out.println(" from in_" + tableName + ";\" > " + filename);
    out.println("gzip " + filename);
  }
}
