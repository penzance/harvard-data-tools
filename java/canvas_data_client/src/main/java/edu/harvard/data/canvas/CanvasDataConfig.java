package edu.harvard.data.canvas;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataConfigurationException;

public class CanvasDataConfig extends DataConfig {

  private final String canvasApiKey;
  private final String canvasApiSecret;
  private final String canvasDataHost;
  private final String dumpInfoDynamoTable;
  private final String tableInfoDynamoTable;

  public CanvasDataConfig(final List<? extends InputStream> streams, final boolean verify)
      throws IOException, DataConfigurationException {
    super(streams, verify);
    this.canvasApiKey = getConfigParameter("canvas_data_api_key", verify);
    this.canvasApiSecret = getConfigParameter("canvas_data_api_secret", verify);
    this.canvasDataHost = getConfigParameter("canvas_data_host", verify);
    this.dumpInfoDynamoTable = getConfigParameter("dump_info_dynamo_table", verify);
    this.tableInfoDynamoTable = getConfigParameter("table_info_dynamo_table", verify);

    this.codeGeneratorScript = "canvas_generate_tools.py";
    this.codeManagerClass = "edu.harvard.data.canvas.CanvasCodeManager";
    if (verify) {
      checkParameters();
    }
  }

  public String getCanvasApiKey() {
    return canvasApiKey;
  }

  public String getCanvasApiSecret() {
    return canvasApiSecret;
  }

  public String getCanvasDataHost() {
    return canvasDataHost;
  }

  public String getDumpInfoDynamoTable() {
    return dumpInfoDynamoTable;
  }

  public String getTableInfoDynamoTable() {
    return tableInfoDynamoTable;
  }
}
