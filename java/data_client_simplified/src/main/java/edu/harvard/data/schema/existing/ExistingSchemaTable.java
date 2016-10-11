package edu.harvard.data.schema.existing;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ExistingSchemaTable {

  private final String sourceTable;
  private final String description;
  private final Integer days;
  private final Integer expireAfterPhase;
  private final Set<String> exclude;
  private String tableName;
  private final String timestampColumn;

  @JsonCreator
  public ExistingSchemaTable(@JsonProperty("source_table") final String sourceTable,
      @JsonProperty("description") final String description,
      @JsonProperty("days") final Integer days,
      @JsonProperty("timestamp_column") final String timestampColumn,
      @JsonProperty("exclude") final List<String> exclude,
      @JsonProperty("expire_after_phase") final Integer expireAfterPhase) {
    this.sourceTable = sourceTable;
    this.description = description;
    this.days = days;
    this.timestampColumn = timestampColumn;
    this.expireAfterPhase = expireAfterPhase;
    this.exclude = new HashSet<String>();
    if (exclude != null) {
      for (final String exclusion : exclude) {
        this.exclude.add(exclusion);
      }
    }
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public String getDescription() {
    return description;
  }

  public Integer getDays() {
    return days;
  }

  public Integer getExpireAfterPhase() {
    return expireAfterPhase;
  }

  public Set<String> getExclude() {
    return exclude;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(final String tableName) {
    this.tableName = tableName;
  }

  public String getTimestampColumn() {
    return timestampColumn;
  }

}
