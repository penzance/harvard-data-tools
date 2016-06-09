package edu.harvard.data.generator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.VerificationException;
import edu.harvard.data.identity.IdentifierType;
import edu.harvard.data.identity.IdentitySchemaTransformer;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.UnexpectedApiResponseException;
import edu.harvard.data.schema.existing.ExistingSchema;
import edu.harvard.data.schema.extension.ExtensionSchema;
import edu.harvard.data.schema.identity.IdentitySchema;

/**
 * Base class for all dataset-specific code generators. It is expected that
 * every data set implementation will provide a subclass of this abstract class
 * in order to generate its SDK, Hadoop identity jobs and other scripts and
 * code.
 * <P>
 * This class uses the Abstract Factory design pattern. It implements the schema
 * transformation and code generation functionality by calling a set of abstract
 * methods defined in the class itself. These abstract methods represent the
 * minimal amount of information that must be provided by an implementing
 * subclass.
 */
public abstract class CodeGenerator {

  private static final Logger log = LogManager.getLogger();

  protected final File codeDir;
  private final S3ObjectId workingDir;
  private final DataConfig config;

  /**
   * Initialize the CodeGenerator class with input and output file locations.
   *
   * @param codeDir
   *          the directory where generated files should be stored. If this
   *          directory does not exist it will be created.
   * @param workingDir
   *          S3 scratch directory to store intermediate code and data during
   *          the pipeline's run
   */
  public CodeGenerator(final DataConfig config, final File codeDir, final S3ObjectId workingDir) {
    this.config = config;
    this.codeDir = codeDir;
    this.workingDir = workingDir;
  }

  /**
   * Generate and populate a {@link GenerationSpec} object that contains the
   * parameters and identifiers to be used by generated code. See the
   * documentation for {@link GenerationSpec} for details on the fields that may
   * be added to the spec.
   *
   * @return a populated {@code GenerationSpec} object that will be used to
   *         guide the code generator.
   *
   * @throws IOException
   *           if an error occurs when reading or transforming the data schema.
   * @throws DataConfigurationException
   *           if the subclass encounters a problem when reading some
   *           client-specific configuration file.
   * @throws VerificationException
   *           if an error is detected in the the input schema specifications.
   * @throws UnexpectedApiResponseException
   *           if the subclass encounters an error when interfacing with an
   *           external API.
   */
  protected abstract GenerationSpec createGenerationSpec() throws IOException,
  DataConfigurationException, VerificationException, UnexpectedApiResponseException;

  /**
   * Get the name of a resource on the class path that contains the definitions
   * of any existing Redshift tables that should be loaded into the data
   * pipeline before processing begins. See {@link ExistingSchema} for details
   * on the format of this resource.
   *
   * @return a {@code String} containing the name of a classpath resource. A
   *         resource by this name must be available to the classloader that
   *         loaded this class.
   */
  protected abstract String getExistingTableResource();

  /**
   * Get the common identifier that is used throughout the data set to uniquely
   * identify an individual. See the description of the
   * {@link edu.harvard.data.identity} package for more details on the main
   * identifier.
   *
   * @return an {@code IdentifierType} that indicates the main identifier used
   *         to differentiate users across the data set.
   */
  protected abstract IdentifierType getMainIdentifier();

  /**
   * Get the name of a resource on the class path that contains the definitions
   * of all user identifiers across the data set. These identifiers will be
   * removed from all models after Phase 1 of processing, and will be used to
   * generate Hadoop jobs to replace identifiers with semantically-meaningless
   * IDs. See {@link IdentitySchema} for details on the format of this resource.
   *
   * @return a {@code String} containing the name of a classpath resource. A
   *         resource by this name must be available to the classloader that
   *         loaded this class.
   */
  protected abstract String getIdentifierResource();

  /**
   * Get the name of a resource on the class path that contains the definitions
   * of any new or modified table that will be produced as a result of running
   * Phase 2. This resource will guide the generation of the Phase 2 models and
   * all later scripts. See {@link ExtensionSchema} for details on the format of
   * this resource.
   *
   * @return a {@code String} containing the name of a classpath resource. A
   *         resource by this name must be available to the classloader that
   *         loaded this class.
   */
  protected abstract String getPhaseTwoAdditionsResource();

  /**
   * Get the name of a resource on the class path that contains the definitions
   * of any new or modified table that will be produced as a result of running
   * Phase 3. This resource will guide the generation of the Phase 3 models and
   * all later scripts. See {@link ExtensionSchema} for details on the format of
   * this resource.
   *
   * @return a {@code String} containing the name of a classpath resource. A
   *         resource by this name must be available to the classloader that
   *         loaded this class.
   */
  protected abstract String getPhaseThreeAdditionsResource();

  /**
   * Generate the various Java, Bash and SQL files that are required by the
   * {@link GenerationSpec}. This is the main entry point to the generator;
   * client code should instantiate a subclass of {@code CodeGenerator} and then
   * call its {@code generate} method to kick off the code generation process.
   *
   * @throws IOException
   *           if an error occurs when reading classpath resources or writing to
   *           generated output files.
   * @throws DataConfigurationException
   *           if the subclass encounters a problem when reading some
   *           client-specific configuration file.
   * @throws VerificationException
   *           if an error is detected in the the input schema specifications.
   * @throws UnexpectedApiResponseException
   *           if the subclass encounters an error when interfacing with an
   *           external API.
   */
  protected final void generate() throws IOException, DataConfigurationException,
  VerificationException, UnexpectedApiResponseException {
    // Fetch the generation spec from the subclass
    final GenerationSpec spec = createGenerationSpec();

    codeDir.mkdirs();
    spec.setOutputBaseDirectory(codeDir);
    spec.setMainIdentifier(getMainIdentifier());

    // Generate the bindings.
    log.info("Generating Java bindings in " + codeDir);
    new JavaBindingGenerator(spec).generate();

    log.info("Generating Java identity Hadoop jobs in " + codeDir);
    new IdentityJobGenerator(spec, IdentitySchema.readIdentities(getIdentifierResource()))
    .generate();

    log.info("Generating Hive table definitions in " + codeDir);
    new CreateHiveTableGenerator(codeDir, spec).generate();

    log.info("Generating Hive query manifests in " + codeDir);
    new HiveQueryManifestGenerator(codeDir, spec).generate();

    log.info("Generating Redshift table definitions in " + codeDir);
    new CreateRedshiftTableGenerator(codeDir, spec, config).generate();

    log.info("Generating Redshift copy from S3 script in " + codeDir);
    new S3ToRedshiftLoaderGenerator(codeDir, spec, config, workingDir).generate();

    log.info("Generating move unmodified files script in " + codeDir);
    new MoveUnmodifiedTableGenerator(codeDir, spec).generate();
  }

  /**
   * Modify a {@link DataSchema} using a series of transformation resources
   * defined by the abstract methods implemented by subclasses of this type. The
   * result of the transformations are a series of {@code DataSchema} objects
   * that represent the state of the data set at the end of each phase.
   * <P>
   * This method should be called by subclass implementations to take advantage
   * of a standardized transformation process over the set of resources defined
   * by the abstract methods of this class.
   *
   * @param base
   *          the original {@code DataSchema} before any transformations are
   *          applied. This can be thought of as the Phase 0 schema,
   *          representing the set of tables either directly obtained from the
   *          data source or minimally transformed in order to fit into the
   *          table structure required by the rest of the pipeline.
   *
   * @return a {@link List} of {@code DataSchema} objects representing the
   *         schema at the end of each phase in the pipeline. Thus to get the
   *         schema at the end of Phase 2, a client would use
   *         {@code transformSchema(base).get(2);}
   *
   * @throws VerificationException
   *           if an error is detected in the the input schema specifications.
   * @throws IOException
   *           if an error occurs when reading classpath resources.
   */
  public final List<DataSchema> transformSchema(final DataSchema base)
      throws VerificationException, IOException {
    // We will transform the schema using the additions specified in json
    // resources.
    final ExtensionSchema phase2 = ExtensionSchema
        .readExtensionSchema(getPhaseTwoAdditionsResource());
    final ExtensionSchema phase3 = ExtensionSchema
        .readExtensionSchema(getPhaseThreeAdditionsResource());

    // Transform the schema to remove identifers
    final IdentitySchema identities = IdentitySchema.readIdentities(getIdentifierResource());
    final IdentitySchemaTransformer idTrans = new IdentitySchemaTransformer(base, identities,
        getMainIdentifier());
    final DataSchema schema1 = idTrans.transform();

    final SchemaTransformer transformer = new SchemaTransformer();
    final DataSchema schema2 = transformer.transform(schema1, phase2);
    final DataSchema schema3 = transformer.transform(schema2, phase3);

    // Add in any tables that are to be read from Redshift. This has to happen
    // last so that we have the correct schema for all existing tables; a table
    // that will be read from Redshift is always based on a table that was
    // previously written there.
    final ExistingTableSchemaTransformer existingTrans = new ExistingTableSchemaTransformer();
    final ExistingSchema existing = ExistingSchema.readExistingSchemas(getExistingTableResource());
    final DataSchema schema0 = existingTrans.transform(base, existing, schema3);

    final List<DataSchema> schemas = new ArrayList<DataSchema>();
    schemas.add(schema0);
    schemas.add(schema1);
    schemas.add(schema2);
    schemas.add(schema3);
    return schemas;
  }

}
