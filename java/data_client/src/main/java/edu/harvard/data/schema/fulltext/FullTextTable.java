package edu.harvard.data.schema.fulltext;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FullTextTable {

  private final String key;
  private final List<String> columns;

  @JsonCreator
  public FullTextTable(@JsonProperty("key") final String key,
      @JsonProperty("columns") final List<String> columnList) {
    this.key = key;
    this.columns = new ArrayList<String>();
    if(columnList != null) {
      columns.addAll(columnList);
    }
  }

  public String getKey() {
    return key;
  }

  public List<String> getColumns() {
    return columns;
  }

}
