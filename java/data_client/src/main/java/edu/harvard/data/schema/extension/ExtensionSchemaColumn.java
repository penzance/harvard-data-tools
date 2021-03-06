package edu.harvard.data.schema.extension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaType;

public class ExtensionSchemaColumn extends DataSchemaColumn {

  private final String name;
  private final String sourceName;
  private final String description;
  private final DataSchemaType type;
  private final Integer length;

  @JsonCreator
  public ExtensionSchemaColumn(@JsonProperty("name") final String name,
      @JsonProperty("description") final String description,
      @JsonProperty("type") final String type, @JsonProperty("length") final Integer length) {
    super(false);
    this.sourceName = name;
    this.name = cleanColumnName(name);
    this.description = description;
    this.type = DataSchemaType.parse(type);
    this.length = length;
  }

  private ExtensionSchemaColumn(final ExtensionSchemaColumn original) {
    super(original.newlyGenerated);
    this.sourceName = original.sourceName;
    this.name = cleanColumnName(original.name);
    this.description = original.description;
    this.type = original.type;
    this.length = original.length;
  }

  @Override
  public DataSchemaColumn copy() {
    return new ExtensionSchemaColumn(this);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public DataSchemaType getType() {
    return type;
  }

  @Override
  public Integer getLength() {
    return length;
  }

  @Override
  public String toString() {
    return name + ": " + type + " (" + length + ") " + (newlyGenerated ? "*":"");
  }

  @Override
  public String getSourceName() {
    return sourceName;
  }

}
