package edu.harvard.data.canvas;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.pipeline.DataConfig;

public class CanvasDataConfig extends DataConfig {

  public final String canvasApiKey;
  public final String canvasApiSecret;
  public final String canvasDataHost;
  public final String dumpInfoDynamoTable;
  public final String tableInfoDynamoTable;

  public CanvasDataConfig(final List<? extends InputStream> streams, final boolean verify)
      throws IOException, DataConfigurationException {
    super(streams, verify);
    this.canvasApiKey = getConfigParameter("canvas_data_api_key", verify);
    this.canvasApiSecret = getConfigParameter("canvas_data_api_secret", verify);
    this.canvasDataHost = getConfigParameter("canvas_data_host", verify);
    this.dumpInfoDynamoTable = getConfigParameter("dump_info_dynamo_table", verify);
    this.tableInfoDynamoTable = getConfigParameter("table_info_dynamo_table", verify);
  }

}
