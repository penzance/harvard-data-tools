package edu.harvard.data.client;

import java.io.IOException;

import edu.harvard.data.client.canvas.CanvasApiClient;
import edu.harvard.data.client.schema.DataSchema;
import edu.harvard.data.client.schema.UnexpectedApiResponseException;

public abstract class DataClient {

  public static CanvasApiClient getCanvasApiClient(final String host, final String key,
      final String secret) {
    return new CanvasApiClient(host, key, secret);
  }

  public abstract DataSchema getSchema(final String version)
      throws DataConfigurationException, UnexpectedApiResponseException, IOException;
}
