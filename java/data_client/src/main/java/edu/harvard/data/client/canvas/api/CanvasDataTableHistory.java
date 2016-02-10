package edu.harvard.data.client.canvas.api;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CanvasDataTableHistory {

  private final String table;
  private final List<CanvasDataHistoricalDump> history;

  @JsonCreator
  public CanvasDataTableHistory(@JsonProperty("table") final String tableName,
      @JsonProperty("history") final List<CanvasDataHistoricalDump> history) {
    this.table = tableName;
    this.history = history;
  }

  public String getTableName() {
    return table;
  }

  public List<CanvasDataHistoricalDump> getHistory() {
    return Collections.unmodifiableList(history);
  }

  public void setRestUtils(final RestUtils rest) {
    for (final CanvasDataHistoricalDump dump : history) {
      dump.setRestUtils(rest);
    }
  }

}
