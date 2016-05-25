package edu.harvard.data.generator;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.UnexpectedApiResponseException;
import edu.harvard.data.schema.extension.ExtensionSchema;

public class CodeGeneratorIntegrationTests {

  private static final File TEMP_DIR = new File("/tmp/CodeGeneratorIntegrationTests");
  private static final String BINDINGS_PREFIX = "java/src/main/java/edu/harvard/data/integration/bindings";
  private static final String IDENTITY_PREFIX = "java/src/main/java/edu/harvard/data/integration/identity";
  private static File hiveDir;
  private static File codeDir;
  private static TestCodeGenerator gen;
  private static List<String> originalSchemaTables;

  @BeforeClass
  public static void setup() throws IOException, VerificationException, DataConfigurationException,
  UnexpectedApiResponseException {
    clearTempDir();
    hiveDir = new File(TEMP_DIR, "hive");
    codeDir = new File(TEMP_DIR, "generated_code");

    originalSchemaTables = new ArrayList<String>();
    originalSchemaTables.add("AllPossibleColumnTypes");
    originalSchemaTables.add("TableWithIdentifier");
    originalSchemaTables.add("TableWithMultiplexedIdentifier");

    new File(hiveDir, "phase_2").mkdirs();
    new File(hiveDir, "phase_3").mkdirs();
    new File(hiveDir, "phase_2/phase_2_hive_file.q").createNewFile();
    new File(hiveDir, "phase_2/phase_2_other_hive_file.q").createNewFile();
    new File(hiveDir, "phase_3/phase_3_hive_file.q").createNewFile();

    final DataSchema schema = ExtensionSchema
        .readExtensionSchema("code_generator_integration_tests/initial_schema.json");
    gen = new TestCodeGenerator(codeDir, hiveDir, schema);
    gen.generate();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    // clearTempDir();
  }

  private static void clearTempDir() throws IOException {
    if (TEMP_DIR.exists()) {
      FileUtils.deleteDirectory(TEMP_DIR);
    }
  }

  private void checkForFiles(final File dir, final List<String> expectedFiles) {
    final List<String> foundFiles = new ArrayList<String>();
    for (final String file : dir.list()) {
      foundFiles.add(file);
    }
    for (final String file : expectedFiles) {
      assertThat(foundFiles, hasItem(file));
    }
    assertEquals(expectedFiles.size(), foundFiles.size());
  }

  // Check that all expected files are in the root directory. We will check the
  // Java files in another test.
  @Test
  public void testRootFiles() throws IOException, DataConfigurationException, VerificationException,
  UnexpectedApiResponseException {
    final List<String> expectedFiles = new ArrayList<String>();
    expectedFiles.add("phase_2_create_tables.sh");
    expectedFiles.add("phase_3_create_tables.sh");
    expectedFiles.add("phase_2_hive.sh");
    expectedFiles.add("phase_3_hive.sh");
    expectedFiles.add("create_redshift_tables.sql");
    expectedFiles.add("s3_to_redshift_loader.sql");
    expectedFiles.add("phase_1_move_unmodified_files.sh");
    expectedFiles.add("phase_2_move_unmodified_files.sh");
    expectedFiles.add("phase_3_move_unmodified_files.sh");
    expectedFiles.add("java");
    checkForFiles(codeDir, expectedFiles);
  }

  // Check that all regular Java files and directories are present. We will
  // check the table-specific files in another test.
  @Test
  public void testStandardJavaFiles() {
    final File dir = new File(codeDir, "java");
    final List<String> expectedFiles = new ArrayList<String>();
    expectedFiles.add("pom.xml");
    expectedFiles.add("src");
    checkForFiles(dir, expectedFiles);
  }

  // Check that phase 0 table-specific binding files are all present
  @Test
  public void testPhase0BindingFiles() {
    final File dir = new File(codeDir, BINDINGS_PREFIX + "/phase0");
    final List<String> expectedFiles = new ArrayList<String>();
    for (final String originalTable : originalSchemaTables) {
      expectedFiles.add("Phase0" + originalTable + ".java");
    }
    expectedFiles.add("Phase0ItegrationTestTable.java");
    expectedFiles.add("Phase0ItegrationTestTableFactory.java");
    checkForFiles(dir, expectedFiles);
  }

  // Check that phase 1 table-specific binding files are all present
  @Test
  public void testPhase1BindingFiles() {
    final File dir = new File(codeDir, BINDINGS_PREFIX + "/phase1");
    final List<String> expectedFiles = new ArrayList<String>();
    for (final String originalTable : originalSchemaTables) {
      expectedFiles.add("Phase1" + originalTable + ".java");
    }
    expectedFiles.add("Phase1ItegrationTestTable.java");
    expectedFiles.add("Phase1ItegrationTestTableFactory.java");
    checkForFiles(dir, expectedFiles);
  }

  // Check that phase 2 table-specific binding files are all present
  @Test
  public void testPhase2BindingFiles() {
    final File dir = new File(codeDir, BINDINGS_PREFIX + "/phase2");
    final List<String> expectedFiles = new ArrayList<String>();
    for (final String originalTable : originalSchemaTables) {
      expectedFiles.add("Phase2" + originalTable + ".java");
    }
    expectedFiles.add("Phase2ItegrationTestTable.java");
    expectedFiles.add("Phase2ItegrationTestTableFactory.java");
    expectedFiles.add("Phase2LikeTable.java");
    expectedFiles.add("Phase2LikeTableWithAdditions.java");
    expectedFiles.add("Phase2SimpleTable.java");
    checkForFiles(dir, expectedFiles);
  }

  // Check that phase 3 table-specific binding files are all present
  @Test
  public void testPhase3BindingFiles() {
    final File dir = new File(codeDir, BINDINGS_PREFIX + "/phase3");
    final List<String> expectedFiles = new ArrayList<String>();
    for (final String originalTable : originalSchemaTables) {
      expectedFiles.add("Phase3" + originalTable + ".java");
    }
    expectedFiles.add("Phase3ItegrationTestTable.java");
    expectedFiles.add("Phase3ItegrationTestTableFactory.java");
    expectedFiles.add("Phase3LikeTable.java");
    expectedFiles.add("Phase3LikeTableWithAdditions.java");
    expectedFiles.add("Phase3SimpleTable.java");
    checkForFiles(dir, expectedFiles);
  }

  // Check that identity hadoop job files are all present
  @Test
  public void testIdentityHadoopFiles() {
    final File dir = new File(codeDir, IDENTITY_PREFIX);
    final List<String> expectedFiles = new ArrayList<String>();
    expectedFiles.add("IntegrationTestIdentityHadoopManager.java");
    checkForFiles(dir, expectedFiles);
  }

  private void compareFiles(final File genFile) throws FileNotFoundException, IOException {
    final String resource = "code_generator_integration_tests/expected/" + genFile.getName();
    final ClassLoader cl = this.getClass().getClassLoader();
    try (BufferedReader generated = new BufferedReader(new FileReader(genFile));
        InputStream in = cl.getResourceAsStream(resource);
        BufferedReader expected = new BufferedReader(new InputStreamReader(in))) {
      final List<String> expectedData = readCodeFile(expected);
      final List<String> generatedData = readCodeFile(generated);
      for (int i = 0; i < expectedData.size(); i++) {
        if (generatedData.size() <= i) {
          fail("Generated file does not contain line " + expectedData.get(i));
        }
        assertEquals("Mismatch at line " + i, expectedData.get(i), generatedData.get(i));
      }
      if (generatedData.size() > expectedData.size()) {
        String msg = "Generated file contains additional lines:";
        for (int i = expectedData.size(); i < generatedData.size(); i++) {
          msg += generatedData.get(i) + "\n";
        }
        fail(msg);
      }
    }
  }

  // Read a stream line by line, trimming each line and ignoring those with no
  // content.
  private List<String> readCodeFile(final BufferedReader in) throws IOException {
    final List<String> lines = new ArrayList<String>();
    String line = in.readLine();
    while (line != null) {
      line = line.trim();
      if (line.length() > 0) {
        lines.add(line);
      }
      line = in.readLine();
    }
    return lines;
  }

  // Compare phase_2_create_tables.sh file to expected output
  @Test
  public void testPhase2CreateTables() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, "phase_2_create_tables.sh"));
  }

  // Compare phase_3_create_tables.sh file to expected output
  @Test
  public void testPhase3CreateTables() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, "phase_3_create_tables.sh"));
  }

  // Compare phase_2_hive.sh file to expected output
  @Test
  public void testPhase2Hive() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, "phase_2_hive.sh"));
  }

  // Compare phase_3_hive.sh file to expected output
  @Test
  public void testPhase3Hive() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, "phase_3_hive.sh"));
  }

  // Compare create_redshift_tables.sql file to expected output
  @Test
  public void testCreateRedshiftTables() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, "create_redshift_tables.sql"));
  }

  // Compare s3_to_redshift_loader.sh file to expected output
  @Test
  public void testS3ToRedshiftLoader() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, "s3_to_redshift_loader.sql"));
  }

  // Compare phase_1_move_unmodified_files.sh file to expected output
  @Test
  public void testPhase1MoveUnmodifiedFiles() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, "phase_1_move_unmodified_files.sh"));
  }

  // Compare phase_2_move_unmodified_files.sh file to expected output
  @Test
  public void testPhase2MoveUnmodifiedFiles() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, "phase_2_move_unmodified_files.sh"));
  }

  // Compare phase_3_move_unmodified_files.sh file to expected output
  @Test
  public void testPhase3MoveUnmodifiedFiles() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, "phase_3_move_unmodified_files.sh"));
  }

  // Compare Phase2SimpleTable.java file to expected output
  @Test
  public void testPhase2SimpleTable() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, BINDINGS_PREFIX + "/phase2/Phase2SimpleTable.java"));
  }

  // Compare Phase3SimpleTable.java file to expected output
  @Test
  public void testPhase3SimpleTable() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, BINDINGS_PREFIX + "/phase3/Phase3SimpleTable.java"));
  }

  // Compare Phase2LikeTable.java file to expected output
  @Test
  public void testPhase2LikeTable() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, BINDINGS_PREFIX + "/phase2/Phase2LikeTable.java"));
  }

  // Compare Phase3LikeTable.java file to expected output
  @Test
  public void testPhase3LikeTable() throws FileNotFoundException, IOException {
    compareFiles(new File(codeDir, BINDINGS_PREFIX + "/phase3/Phase3LikeTable.java"));
  }

}
// Add temporary tables
// Add like a table from a previous phase.

class TestCodeGenerator extends CodeGenerator {

  static final String PHASE_ZERO_PACKAGE = "edu.harvard.data.integration.bindings.phase0";
  static final String PHASE_ONE_PACKAGE = "edu.harvard.data.integration.bindings.phase1";
  static final String PHASE_TWO_PACKAGE = "edu.harvard.data.integration.bindings.phase2";
  static final String PHASE_THREE_PACKAGE = "edu.harvard.data.integration.bindings.phase3";
  static final String IDENTITY_HADOOP_PACKAGE = "edu.harvard.data.integration.identity";
  private final File hiveDir;
  private final DataSchema schema0;

  public TestCodeGenerator(final File codeDir, final File hiveDir, final DataSchema schema0)
      throws FileNotFoundException {
    super(codeDir);
    this.hiveDir = hiveDir;
    this.schema0 = schema0;
  }

  @Override
  protected GenerationSpec createGenerationSpec() throws IOException, DataConfigurationException,
  VerificationException, UnexpectedApiResponseException {
    final GenerationSpec spec = new GenerationSpec(2);
    spec.setJavaProjectName("integration_test_generated_code");
    spec.setJavaTableEnumName("ItegrationTestTable");
    spec.setPrefixes("Phase0", "Phase1", "Phase2", "Phase3");
    spec.setHdfsDirectories("hdfs_0", "hdfs_1", "hdfs_2", "hdfs_3");
    spec.setJavaBindingPackages(PHASE_ZERO_PACKAGE, PHASE_ONE_PACKAGE, PHASE_TWO_PACKAGE,
        PHASE_THREE_PACKAGE);
    spec.setJavaHadoopPackage(IDENTITY_HADOOP_PACKAGE);
    spec.setHadoopIdentityManagerClass("IntegrationTestIdentityHadoopManager");
    spec.setHiveScriptDir(hiveDir);
    final List<DataSchema> schemas = transformSchema(schema0);
    spec.setSchemas(schemas.get(0), schemas.get(1), schemas.get(2), schemas.get(3));
    return spec;
  }

  @Override
  protected String getExistingTableResource() {
    return "code_generator_integration_tests/existing_tables.json";
  }

  @Override
  protected IdentifierType getMainIdentifier() {
    return IdentifierType.XID;
  }

  @Override
  protected String getIdentifierResource() {
    return "code_generator_integration_tests/identifiers.json";
  }

  @Override
  protected String getPhaseTwoAdditionsResource() {
    return "code_generator_integration_tests/phase_2_additional_resources.json";
  }

  @Override
  protected String getPhaseThreeAdditionsResource() {
    return "code_generator_integration_tests/phase_3_additional_resources.json";
  }

}