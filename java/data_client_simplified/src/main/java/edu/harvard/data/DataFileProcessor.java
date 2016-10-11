package edu.harvard.data;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.identity.IdentityService;
import edu.harvard.data.io.TableReader;
import edu.harvard.data.io.TableWriter;

public class DataFileProcessor implements Callable<Void> {

  private final S3ObjectId input;
  private final S3ObjectId output;
  private final IdentityService idService;
  private final DataConfig config;
  private final CodeManager codeManager;
  private final String tableName;
  private final TableFormat format;

  public DataFileProcessor(final S3ObjectId input, final S3ObjectId output, final String tableName,
      final IdentityService idService, final CodeManager codeManager, final DataConfig config) {
    this.input = input;
    this.output = output;
    this.tableName = tableName;
    this.idService = idService;
    this.codeManager = codeManager;
    this.config = config;
    this.format = new FormatLibrary().getFormat(config.getPipelineFormat());
  }

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Void call() throws Exception {
    final File tmpDir = new File(config.getScratchDir() + "/event");
    final File tmpFile = new File(tmpDir, UUID.randomUUID().toString());

    try (
        TableReader<? extends DataTable> in = codeManager.getS3InputTableReader(tableName, format,
            input, tmpDir);
        CloseableMap<String, TableWriter<? extends DataTable>> outputs = codeManager.getWriters(tableName, format,
            output, tmpFile)) {
      final DataFilter idFilter = codeManager.getIdentityFilter(tableName, idService, config);
      final DataFilter textFilter = codeManager.getFullTextFilter(tableName, config);
      final DataOutput output = codeManager.getDataOutput(tableName, config);
      for (final DataTable inputRecord : in) {
        final DataTable deIdentified = idFilter.filter(inputRecord);
        final DataTable fullText = textFilter.filter(deIdentified);
        output.output(fullText, outputs);
      }
    }
    return null;
  }

}
