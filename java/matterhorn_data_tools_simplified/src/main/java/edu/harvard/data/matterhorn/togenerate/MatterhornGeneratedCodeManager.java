package edu.harvard.data.matterhorn.togenerate;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataFilter;
import edu.harvard.data.identity.IdentityService;

public class MatterhornGeneratedCodeManager {

  public DataFilter getIdentityFilter(final String tableName, final IdentityService idService,
      final DataConfig config) {
    switch (tableName) {
    case "event":
      return new EventIdentityFilter(idService, config);
    default:
      throw new RuntimeException("Unknown table " + tableName);
    }
  }

  public DataFilter getFullTextFilter(final String tableName, final DataConfig config) {
    switch (tableName) {
    case "event":
      return new EventFullTextFilter(config);
    default:
      throw new RuntimeException("Unknown table " + tableName);
    }
  }
}
