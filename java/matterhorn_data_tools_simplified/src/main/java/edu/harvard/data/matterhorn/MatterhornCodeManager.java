package edu.harvard.data.matterhorn;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.CodeManager;
import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.DataTable;
import edu.harvard.data.DownloadAndVerify;
import edu.harvard.data.ProcessingStep;
import edu.harvard.data.TableFormat;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.matterhorn.bindings.phase_0.Phase0MatterhornTableFactory;
import edu.harvard.data.matterhorn.bindings.phase_1.Phase1MatterhornTableFactory;

public class MatterhornCodeManager extends CodeManager {

  public MatterhornCodeManager() {
    super(new MatterhornGeneratedCodeManager(), new Phase0MatterhornTableFactory(),
        new Phase1MatterhornTableFactory());
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
  public Map<String, TableWriter<DataTable>> getAdditionalWriters(final String tableName,
      final TableFormat format, final S3ObjectId output, final File tmpFile) throws IOException {
    return new HashMap<String, TableWriter<DataTable>>();
  }

  @Override
  public List<ProcessingStep> getCustomSteps(final String tableName, final DataConfig config) {
    return new ArrayList<ProcessingStep>();
  }

}
