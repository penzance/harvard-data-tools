package edu.harvard.data.data_tools.canvas;

import java.io.IOException;

import edu.harvard.data.client.DataClient;
import edu.harvard.data.client.DataConfiguration;
import edu.harvard.data.client.DataConfigurationException;
import edu.harvard.data.client.canvas.CanvasApiClient;
import edu.harvard.data.client.canvas.CanvasDataArtifact;
import edu.harvard.data.client.canvas.CanvasDataDump;
import edu.harvard.data.client.schema.UnexpectedApiResponseException;
import edu.harvard.data.data_tools.Command;
import edu.harvard.data.data_tools.ReturnStatus;
import edu.harvard.data.data_tools.TableInfo;
import edu.harvard.data.data_tools.VerificationException;

public class CanvasGetCompleteTableInfoCommand implements Command {

  @Override
  public ReturnStatus execute(final DataConfiguration config) throws IOException,
  UnexpectedApiResponseException, DataConfigurationException, VerificationException {
    final CanvasApiClient api = DataClient.getCanvasApiClient(config.getCanvasDataHost(),
        config.getCanvasApiKey(), config.getCanvasApiSecret());
    final CanvasDataDump dump = api.getLatestDump();
    for (final CanvasDataArtifact artifact : dump.getArtifactsByTable().values()) {
      final String tableName = artifact.getTableName();
      final TableInfo info = TableInfo.find(tableName);
      System.out.println(tableName + ": " + info.getLastCompleteDumpSequence());
    }
    return ReturnStatus.OK;
  }

  @Override
  public String getDescription() {
    return "Get a JSON list of tables, with the latest non-incremental dump";
  }

}
