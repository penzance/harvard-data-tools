package edu.harvard.data;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.identity.IdentityService;
import edu.harvard.data.io.TableReader;
import edu.harvard.data.io.TableWriter;

public abstract class CodeManager {

  private final GeneratedCodeManager generatedManager;
  private final AwsUtils aws;
  private final TableFactory inputTableFactory;
  private final TableFactory outputTableFactory;

  protected CodeManager(final GeneratedCodeManager generatedManager,
      final TableFactory inputTableFactory, final TableFactory outputTableFactory) {
    this.generatedManager = generatedManager;
    this.inputTableFactory = inputTableFactory;
    this.outputTableFactory = outputTableFactory;
    this.aws = new AwsUtils();

  }

  public abstract DataConfig getDataConfig(final String configPathString, final boolean verify)
      throws IOException, DataConfigurationException;

  public abstract DownloadAndVerify getDownloadAndVerify(DataConfig config, final String datasetId,
      final String runId, final ExecutorService exec)
          throws IOException, DataConfigurationException;

  public abstract List<ProcessingStep> getCustomSteps(String tableName, DataConfig config);

  public abstract Map<String, TableWriter<DataTable>> getAdditionalWriters(final String tableName,
      final TableFormat format, final S3ObjectId output, final File tmpFile) throws IOException;

  @SuppressWarnings("unchecked")
  public static CodeManager getCodeManager(final String codeManagerClassName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    final Class<? extends CodeManager> codeManagerClass = (Class<? extends CodeManager>) Class
        .forName(codeManagerClassName);
    return codeManagerClass.newInstance();
  }

  public ProcessingStep getIdentityStep(final String tableName, final IdentityService idService,
      final DataConfig config) {
    return generatedManager.getIdentityStep(tableName, idService, config);
  }

  public ProcessingStep getFullTextStep(final String tableName) {
    return generatedManager.getFullTextStep(tableName);
  }

  public TableReader<? extends DataTable> getS3InputTableReader(final String tableName,
      final TableFormat format, final S3ObjectId input, final File tmpDir) throws IOException {
    return inputTableFactory.getTableReader(tableName, format, aws, input, tmpDir);
  }

  @SuppressWarnings("unchecked")
  public CloseableMap<String, TableWriter<DataTable>> getWriters(final String tableName,
      final TableFormat format, final S3ObjectId output, final File tmpFile) throws IOException {
    final Map<String, TableWriter<DataTable>> map = new HashMap<String, TableWriter<DataTable>>();
    map.put(tableName, (TableWriter<DataTable>) outputTableFactory.getTableWriter(tableName, format,
        output, tmpFile));
    if (getFullTextStep(tableName) != null) {
      map.putAll(generatedManager.getFullTextWriters(inputTableFactory, tableName, format, tmpFile.toString()));
    }
    map.putAll(getAdditionalWriters(tableName, format, output, tmpFile));
    return new CloseableMap<String, TableWriter<DataTable>>(map);
  }

}
