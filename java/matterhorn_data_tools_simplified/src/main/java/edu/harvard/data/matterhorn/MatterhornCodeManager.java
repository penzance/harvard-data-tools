package edu.harvard.data.matterhorn;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.CloseableMap;
import edu.harvard.data.CodeManager;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DataFilter;
import edu.harvard.data.DataOutput;
import edu.harvard.data.DataTable;
import edu.harvard.data.DownloadAndVerify;
import edu.harvard.data.TableFormat;
import edu.harvard.data.identity.IdentityService;
import edu.harvard.data.io.TableReader;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.matterhorn.bindings.input.InputMatterhornTableFactory;
import edu.harvard.data.matterhorn.bindings.output.OutputEvent;
import edu.harvard.data.matterhorn.togenerate.MatterhornGeneratedCodeManager;
import edu.harvard.data.matterhorn.togenerate.ToGenerateOutputMatterhornTableFactory;

public class MatterhornCodeManager extends CodeManager {

  private final MatterhornGeneratedCodeManager generatedManager;
  private final AwsUtils aws;
  private final InputMatterhornTableFactory inputTableFactory;
  private final ToGenerateOutputMatterhornTableFactory outputTableFactory;

  public MatterhornCodeManager() {
    this.generatedManager = new MatterhornGeneratedCodeManager();
    this.inputTableFactory = new InputMatterhornTableFactory();
    this.outputTableFactory = new ToGenerateOutputMatterhornTableFactory();
    this.aws = new AwsUtils();
  }

  @Override
  public DataConfig getDataConfig(final String configPathString, final boolean verify)
      throws IOException, DataConfigurationException {
    return MatterhornDataConfig.parseInputFiles(MatterhornDataConfig.class, configPathString,
        verify);
  }

  @Override
  public DownloadAndVerify getDownloadAndVerify(final DataConfig config, final String datasetId,
      final String runId, final ExecutorService exec)
          throws IOException, DataConfigurationException {
    return new MatterhornDownloadAndVerify((MatterhornDataConfig) config, runId, exec);
  }

  @Override
  public DataFilter getIdentityFilter(final String tableName, final IdentityService idService,
      final DataConfig config) {
    return generatedManager.getIdentityFilter(tableName, idService, config);
  }

  @Override
  public TableReader<? extends DataTable> getS3InputTableReader(final String tableName,
      final TableFormat format, final S3ObjectId input, final File tmpDir) throws IOException {
    return inputTableFactory.getTableReader(tableName, format, aws, input, tmpDir);
  }

  @Override
  public DataFilter getFullTextFilter(final String tableName, final DataConfig config) {
    return generatedManager.getFullTextFilter(tableName, config);
  }

  @Override
  public CloseableMap<String, TableWriter<? extends DataTable>> getWriters(final String tableName,
      final TableFormat format, final S3ObjectId output, final File tmpFile) {
    final Map<String, TableWriter<? extends DataTable>> map = new HashMap<String, TableWriter<? extends DataTable>>();
    map.put(tableName, outputTableFactory.getTableWriter(tableName, format, output, tmpFile));
    return new CloseableMap<String, TableWriter<? extends DataTable>>(map);
  }

  @Override
  public DataOutput<? extends DataTable> getDataOutput(final String tableName,
      final DataConfig config) {
    // Override DataOutput for custom behavior.
    return new DataOutput<OutputEvent>(config, OutputEvent.class);
  }

}
