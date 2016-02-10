package edu.harvard.data.client;

import edu.harvard.data.client.canvas.api.CanvasApiClient;

public class DataClient {

  public CanvasApiClient getCanvasApiClient(final String host, final String key,
      final String secret) {
    return new CanvasApiClient(host, key, secret);
  }

}
