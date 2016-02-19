package edu.harvard.data.generator;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.client.DataClient;
import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.schema.DataSchema;
import edu.harvard.data.client.schema.UnexpectedApiResponseException;
import edu.harvard.data.generator.bash.HDFSCopyUnmodifiedTableGenerator;
import edu.harvard.data.generator.hive.CreateHiveTableGenerator;
import edu.harvard.data.generator.hive.HiveQueryManifestGenerator;
import edu.harvard.data.generator.java.JavaBindingGenerator;
import edu.harvard.data.generator.redshift.CreateRedshiftTableGenerator;
import edu.harvard.data.generator.redshift.S3ToRedshiftLoaderGenerator;

// Classpath must include directories containing secure.properties, and the two additions.json files
public class CanvasDataGenerator {
  private static final Logger log = LogManager.getLogger();

  public static final String HDFS_HIVE_QUERY_DIR = "HIVE_QUERY_DIR";
  public static final String HDFS_PHASE_0_DIR = "HDFS_PHASE_0_DIR";
  public static final String HDFS_PHASE_1_DIR = "HDFS_PHASE_1_DIR";
  public static final String HDFS_PHASE_2_DIR = "HDFS_PHASE_2_DIR";

  public static final String CLIENT_PACKAGE = "edu.harvard.data.client";
  private static final String PHASE_ZERO_PACKAGE = CLIENT_PACKAGE + ".canvas.phase0";
  private static final String PHASE_ONE_PACKAGE = CLIENT_PACKAGE + ".canvas.phase1";
  private static final String PHASE_TWO_PACKAGE = CLIENT_PACKAGE + ".canvas.phase2";

  static final String PHASE_ONE_ADDITIONS_JSON = "phase1_schema_additions.json";
  static final String PHASE_TWO_ADDITIONS_JSON = "phase2_schema_additions.json";

  public static void main(final String[] args)
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
    if (args.length != 3) {
      System.err
      .println("Usage: schema_version /path/to/harvard-data-tools /path/to/output/directory");
    }
    final String schemaVersion = args[0];
    final File gitDir = new File(args[1]);
    final File dir = new File(args[2]);
    dir.mkdirs();

    // Get the specified schema version (or fail if that version doesn't exist).
    final DataConfiguration config = DataConfiguration.getConfiguration("secure.properties");
    final String host = config.getCanvasDataHost();
    final String key = config.getCanvasApiKey();
    final String secret = config.getCanvasApiSecret();
    final DataSchema schema = DataClient.getCanvasApiClient(host, key, secret)
        .getSchema(schemaVersion);

    // Specify the three versions of the table bindings
    final SchemaTransformer transformer = new SchemaTransformer(3);
    transformer.setPrefixes("", "Phase1", "Phase2");
    transformer.setSchemas(schema, PHASE_ONE_ADDITIONS_JSON, PHASE_TWO_ADDITIONS_JSON);
    transformer.setHdfsDirectories(HDFS_PHASE_0_DIR, HDFS_PHASE_1_DIR, HDFS_PHASE_2_DIR);

    // Figure out the Java directory and package structure
    final File javaBase = new File(dir, "java");
    javaBase.mkdir();
    final File javaSrcBase = new File(javaBase, "src/main/java");
    final File javaPhase0Dir = new File(javaSrcBase,
        PHASE_ZERO_PACKAGE.replaceAll("\\.", File.separator));
    final File javaPhase1Dir = new File(javaSrcBase,
        PHASE_ONE_PACKAGE.replaceAll("\\.", File.separator));
    final File javaPhase2Dir = new File(javaSrcBase,
        PHASE_TWO_PACKAGE.replaceAll("\\.", File.separator));
    transformer.setJavaPackages(PHASE_ZERO_PACKAGE, PHASE_ONE_PACKAGE, PHASE_TWO_PACKAGE);
    transformer.setJavaSourceLocations(javaPhase0Dir, javaPhase1Dir, javaPhase2Dir);

    // Generate the bindings
    log.info("Generating Java bindings in " + dir);
    new JavaBindingGenerator(javaBase, transformer).generate();

    log.info("Generating Hive table definitions in " + dir);
    new CreateHiveTableGenerator(dir, transformer).generate();

    log.info("Generating Hive query manifests in " + dir);
    new HiveQueryManifestGenerator(gitDir, dir, transformer).generate();

    log.info("Generating Redshift table definitions in " + dir);
    new CreateRedshiftTableGenerator(dir, transformer).generate();

    log.info("Generating Redshift copy from S3 script in " + dir);
    new S3ToRedshiftLoaderGenerator(dir, transformer).generate();

    log.info("Generating Redshift copy from S3 script in " + dir);
    new HDFSCopyUnmodifiedTableGenerator(dir, transformer).generate();
  }

}
