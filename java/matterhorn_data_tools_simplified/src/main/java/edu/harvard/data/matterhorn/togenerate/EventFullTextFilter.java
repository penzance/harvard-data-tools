package edu.harvard.data.matterhorn.togenerate;

import edu.harvard.data.DataConfig;
import edu.harvard.data.DataFilter;
import edu.harvard.data.DataTable;

public class EventFullTextFilter implements DataFilter {

  public EventFullTextFilter(final DataConfig config) {
  }

  @Override
  public DataTable filter(final DataTable record) {
    return record;
  }

}
