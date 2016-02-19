package edu.harvard.data.generator.java;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.client.AwsUtils;
import edu.harvard.data.client.DataTable;
import edu.harvard.data.client.TableFactory;
import edu.harvard.data.client.TableFormat;
import edu.harvard.data.client.io.FileTableReader;
import edu.harvard.data.client.io.FileTableWriter;
import edu.harvard.data.client.io.S3TableReader;
import edu.harvard.data.client.io.TableReader;
import edu.harvard.data.client.io.TableWriter;
import edu.harvard.data.generator.SchemaPhase;

public class TableFactoryGenerator {

  private static final Logger log = LogManager.getLogger();

  private final String schemaVersion;
  private final SchemaPhase tableVersion;
  private final List<String> tableNames;
  private final String classPrefix;

  public TableFactoryGenerator(final String schemaVersion, final List<String> tableNames,
      final SchemaPhase tableVersion) {
    this.schemaVersion = schemaVersion;
    this.tableNames = tableNames;
    this.tableVersion = tableVersion;
    this.classPrefix = tableVersion.getPrefix();
  }

  public void generate(final PrintStream out) {
    log.info("Generating CanvasTableFactory");
    JavaBindingGenerator.writeFileHeader(out, schemaVersion);
    out.println("package " + tableVersion.getJavaPackage() + ";");
    out.println();

    outputImportStatements(out);
    out.println("public class " + classPrefix + "CanvasTableFactory implements TableFactory {");
    out.println();
    outputFileTableReaderFactory(out);
    outputS3TableReaderFactory(out);
    outputFileTableWriterFactory(out);
    out.println("}");
  }

  private void outputImportStatements(final PrintStream out) {
    out.println("import " + File.class.getName() + ";");
    out.println("import " + IOException.class.getName() + ";");
    out.println();
    out.println("import " + S3ObjectId.class.getName() + ";");
    out.println();
    out.println("import " + AwsUtils.class.getName() + ";");
    out.println("import " + DataTable.class.getName() + ";");
    out.println("import " + FileTableReader.class.getName() + ";");
    out.println("import " + FileTableWriter.class.getName() + ";");
    out.println("import " + S3TableReader.class.getName() + ";");
    out.println("import " + TableFactory.class.getName() + ";");
    out.println("import " + TableFormat.class.getName() + ";");
    out.println("import " + TableReader.class.getName() + ";");
    out.println("import " + TableWriter.class.getName() + ";");
    out.println();
  }

  // Generate a method to create a TableReader for a specific table getting data
  // from a file. The generated method will return an instance of
  // FileTableReader.
  private void outputFileTableReaderFactory(final PrintStream out) {
    final String params = "String table, TableFormat format, File file";
    out.println("  @Override");
    out.println("  public TableReader<? extends DataTable> getTableReader(" + params
        + ") throws IOException {");
    out.println("    switch(table) {");
    for (final String name : tableNames) {
      final String className = JavaBindingGenerator.javaClass(name, classPrefix);
      out.println("    case \"" + name + "\":");
      out.println("      return new FileTableReader<" + className + ">(" + className
          + ".class, format, file, \"" + name + "\");");
    }
    out.println("    }");
    out.println("    return null;");
    out.println("  }");
    out.println();
  }

  // Generate a method to create a TableReader for a specific table getting data
  // from S3. The generated method will return an instance of S3TableReader.
  private void outputS3TableReaderFactory(final PrintStream out) {
    final String params = "final String table, final TableFormat format, final AwsUtils aws, final S3ObjectId obj, final File tempDir";
    out.println("  @Override");
    out.println("  public TableReader<? extends DataTable> getTableReader(" + params
        + ") throws IOException {");
    out.println("    switch(table) {");
    for (final String name : tableNames) {
      final String className = JavaBindingGenerator.javaClass(name, classPrefix);
      out.println("    case \"" + name + "\":");
      out.println("      return new S3TableReader<" + className + ">(aws, " + className
          + ".class, format, obj, \"" + name + "\", tempDir);");
    }
    out.println("    }");
    out.println("    return null;");
    out.println("  }");
    out.println();
  }

  // Generate a method to create a TableWriter for a specific table, writing
  // data to a file. The generated method will return an instance of
  // FileTableWriter.
  private void outputFileTableWriterFactory(final PrintStream out) {
    final String params = "String table, TableFormat format, File file";
    out.println("  @Override");
    out.println("  public TableWriter<? extends DataTable> getTableWriter(" + params
        + ") throws IOException {");
    out.println("    switch(table) {");
    for (final String name : tableNames) {
      final String className = JavaBindingGenerator.javaClass(name, classPrefix);
      out.println("    case \"" + name + "\":");
      out.println("      return new FileTableWriter<" + className + ">(" + className
          + ".class, format, \"" + name + "\", file);");
    }
    out.println("    }");
    out.println("    return null;");
    out.println("  }");
  }

}
