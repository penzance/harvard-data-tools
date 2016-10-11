package edu.harvard.data;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.identity.IdentityService;
import edu.harvard.data.io.TableReader;
import edu.harvard.data.io.TableWriter;

public abstract class CodeManager {

  @SuppressWarnings("unchecked")
  public static CodeManager getCodeManager(final String codeManagerClassName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    final Class<? extends CodeManager> codeManagerClass = (Class<? extends CodeManager>) Class
        .forName(codeManagerClassName);
    return codeManagerClass.newInstance();
  }

  public abstract DataConfig getDataConfig(final String configPathString, final boolean verify)
      throws IOException, DataConfigurationException;

  public abstract DownloadAndVerify getDownloadAndVerify(DataConfig config, final String datasetId,
      final String runId, final ExecutorService exec)
          throws IOException, DataConfigurationException;

  public abstract DataFilter getIdentityFilter(String tableName, IdentityService idService,
      DataConfig config);

  public abstract TableReader<? extends DataTable> getS3InputTableReader(final String tableName,
      final TableFormat format, final S3ObjectId input, final File tmpDir) throws IOException;

  public abstract DataFilter getFullTextFilter(final String tableName, final DataConfig config);

  public abstract CloseableMap<String, TableWriter<? extends DataTable>> getWriters(final String tableName,
      final TableFormat format, final S3ObjectId output, final File tmpFile);

  public abstract DataOutput<? extends DataTable> getDataOutput(final String tableName,
      final DataConfig config);

}
