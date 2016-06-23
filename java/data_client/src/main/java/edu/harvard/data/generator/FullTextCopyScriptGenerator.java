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
      for (final String table : textSchema.tableNames()) {
        if (dataIndex.containsTable(table)) {
          generateTable(out, table);
        }
      }
      out.println("hadoop fs -put " + config.getFullTextDir() + " /full_text");
      out.println("s3-dist-cp --src=hdfs:///full_text --dest="
          + AwsUtils.uri(config.getFullTextLocation()) + " --outputCodec=gzip");
    }
  }

  private void generateTable(final PrintStream out, final String tableName) {
    final FullTextTable table = textSchema.get(tableName);
    out.println("mkdir -p /home/hadoop/full_text/" + tableName);
    for (final String column : table.getColumns()) {
      out.println("sudo hive -S -e \"select " + table.getKey() + ", " + column + " from in_"
          + tableName + ";\" > " + config.getFullTextDir() + "/" + tableName + "/" + column);
    }
    out.println();
  }
}
