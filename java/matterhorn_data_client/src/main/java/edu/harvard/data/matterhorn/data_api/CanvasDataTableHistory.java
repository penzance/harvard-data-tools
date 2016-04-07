package edu.harvard.data.matterhorn.data_api;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CanvasDataTableHistory {

  private final String table;
  private final List<DataHistoricalDump> history;

  @JsonCreator
  public CanvasDataTableHistory(@JsonProperty("table") final String tableName,
      @JsonProperty("history") final List<DataHistoricalDump> history) {
    this.table = tableName;
    this.history = history;
  }

  public String getTableName() {
    return table;
  }

  public List<DataHistoricalDump> getHistory() {
    return Collections.unmodifiableList(history);
  }

  public void setRestUtils(final RestUtils rest) {
    for (final DataHistoricalDump dump : history) {
      dump.setRestUtils(rest);
    }
  }

}
