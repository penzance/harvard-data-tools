package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.VerificationException;
import edu.harvard.data.schema.DataSchemaTable;

public class JavaStepsGenerator {

  private static final Logger log = LogManager.getLogger();

  private final CodeGenerator codeGen;
  private final File javaSrcBase;

  public JavaStepsGenerator(final CodeGenerator codeGen) {
    this.codeGen = codeGen;
    this.javaSrcBase = new File(codeGen.getOutputBase(), "java/src/main/java");

  }

  public void generate() throws IOException, VerificationException {
    final SchemaVersion inputSchema = codeGen.getSchemaVersions().get(0);
    final SchemaVersion outputSchema = codeGen.getSchemaVersions().get(1);

    new File(javaSrcBase, codeGen.getJavaIdentityStepPackage().replaceAll("\\.", File.separator))
    .mkdirs();
    new File(javaSrcBase, codeGen.getJavaFullTextStepPackage().replaceAll("\\.", File.separator))
    .mkdirs();

    for (final String tableName : inputSchema.getSchema().getTableNames()) {
      final DataSchemaTable table = inputSchema.getSchema().getTableByName(tableName);

      final File idFile = getIdFile(tableName);
      log.info("Generating " + idFile);
      try (PrintStream out = new PrintStream(new FileOutputStream(idFile))) {
        final JavaIdentityStepGenerator idGen = new JavaIdentityStepGenerator(codeGen, inputSchema,
            outputSchema, table);
        idGen.generate(out);
      }

      final File textFile = getFullTextFile(tableName);
      log.info("Generating " + textFile);
      try (PrintStream out = new PrintStream(new FileOutputStream(textFile))) {
        final JavaFullTextStepGenerator textGen = new JavaFullTextStepGenerator(codeGen,
            outputSchema, table);
        textGen.generate(out);
      }
    }

    final File textFile = getCodeManagerFile();
    try (PrintStream out = new PrintStream(new FileOutputStream(textFile))) {
      final JavaGeneratedCodeManagerGenerator mgrGen = new JavaGeneratedCodeManagerGenerator(
          codeGen, inputSchema);
      mgrGen.generate(out);
    }
  }

  private File getIdFile(final String tableName) {
    final String pkg = codeGen.getJavaIdentityStepPackage();
    final String cls = JavaIdentityStepGenerator.getClassName(tableName);
    final String path = new String(pkg + "." + cls).replaceAll("\\.", "/") + ".java";
    return new File(javaSrcBase, path);
  }

  private File getFullTextFile(final String tableName) {
    final String pkg = codeGen.getJavaFullTextStepPackage();
    final String cls = JavaFullTextStepGenerator.getClassName(tableName);
    final String path = new String(pkg + "." + cls).replaceAll("\\.", "/") + ".java";
    return new File(javaSrcBase, path);
  }

  private File getCodeManagerFile() {
    final String pkg = codeGen.getJavaPackageBase();
    final String cls = JavaGeneratedCodeManagerGenerator.getClassName(codeGen);
    final String path = new String(pkg + "." + cls).replaceAll("\\.", "/") + ".java";
    return new File(javaSrcBase, path);
  }
}
