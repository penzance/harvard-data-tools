package edu.harvard.data.canvas;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.harvard.data.DataConfiguration;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.canvas.data_api.ApiClient;
import edu.harvard.data.generator.CreateHiveTableGenerator;
import edu.harvard.data.generator.CreateRedshiftTableGenerator;
import edu.harvard.data.generator.GenerationSpec;
import edu.harvard.data.generator.HiveQueryManifestGenerator;
import edu.harvard.data.generator.JavaBindingGenerator;
import edu.harvard.data.generator.MoveUnmodifiedTableGenerator;
import edu.harvard.data.generator.S3ToRedshiftLoaderGenerator;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class CanvasCodeGenerator {
  private static final Logger log = LogManager.getLogger();

  public static final String HDFS_HIVE_QUERY_DIR = "HIVE_QUERY_DIR";
  public static final String HDFS_PHASE_0_DIR = "hdfs:///phase_0";
  public static final String HDFS_PHASE_1_DIR = "hdfs:///phase_1";
  public static final String HDFS_PHASE_2_DIR = "hdfs:///phase_2";
  public static final String HDFS_PHASE_3_DIR = "hdfs:///phase_3";

  public static final String CLIENT_PACKAGE = "edu.harvard.data.canvas.bindings";
  private static final String PHASE_ZERO_PACKAGE = "edu.harvard.data.canvas.bindings.phase0";
  private static final String PHASE_ONE_PACKAGE = "edu.harvard.data.canvas.bindings.phase1";
  private static final String PHASE_TWO_PACKAGE = "edu.harvard.data.canvas.bindings.phase2";
  private static final String PHASE_THREE_PACKAGE = "edu.harvard.data.canvas.bindings.phase3";

  public static final String PHASE_ONE_ADDITIONS_JSON = "phase1_schema_additions.json";
  public static final String PHASE_TWO_ADDITIONS_JSON = "phase2_schema_additions.json";
  public static final String PHASE_THREE_ADDITIONS_JSON = "phase3_schema_additions.json";

  public static void main(final String[] args) throws IOException, DataConfigurationException,
  UnexpectedApiResponseException, SQLException, VerificationException {
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
    final DataSchema schema = new ApiClient(host, key, secret).getSchema(schemaVersion);

    // Specify the four versions of the table bindings
    final GenerationSpec spec = new GenerationSpec(4);
    spec.setOutputBaseDirectory(dir);
    spec.setJavaTableEnumName("CanvasTable");
    spec.setPrefixes("", "Phase1", "Phase2", "Phase3");
    spec.setSchemas(schema, PHASE_ONE_ADDITIONS_JSON, PHASE_TWO_ADDITIONS_JSON,
        PHASE_THREE_ADDITIONS_JSON);
    spec.setHdfsDirectories(HDFS_PHASE_0_DIR, HDFS_PHASE_1_DIR, HDFS_PHASE_2_DIR,
        HDFS_PHASE_3_DIR);
    spec.setJavaPackages(PHASE_ZERO_PACKAGE, PHASE_ONE_PACKAGE, PHASE_TWO_PACKAGE,
        PHASE_THREE_PACKAGE);

    // Generate the bindings.
    log.info("Generating Java bindings in " + dir);
    new JavaBindingGenerator(spec, "canvas_data_schema_bindings").generate();

    log.info("Generating Hive table definitions in " + dir);
    new CreateHiveTableGenerator(dir, spec).generate();

    log.info("Generating Hive query manifests in " + dir);
    new HiveQueryManifestGenerator(gitDir, dir, spec).generate();

    log.info("Generating Redshift table definitions in " + dir);
    new CreateRedshiftTableGenerator(dir, spec).generate();

    log.info("Generating Redshift copy from S3 script in " + dir);
    new S3ToRedshiftLoaderGenerator(dir, spec).generate();

    log.info("Generating move unmodified files script in " + dir);
    new MoveUnmodifiedTableGenerator(dir, spec).generate();
  }

}
