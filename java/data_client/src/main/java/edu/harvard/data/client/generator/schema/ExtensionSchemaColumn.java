package edu.harvard.data.client.generator.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.client.schema.DataSchemaColumn;
import edu.harvard.data.client.schema.DataSchemaType;

public class ExtensionSchemaColumn extends DataSchemaColumn {

  private final String name;
  private final String description;
  private final DataSchemaType type;
  private final Integer length;

  @JsonCreator
  public ExtensionSchemaColumn(@JsonProperty("name") final String name,
      @JsonProperty("description") final String description,
      @JsonProperty("type") final String type, @JsonProperty("length") final int length) {
    super(false);
    this.name = cleanColumnName(name);
    this.description = description;
    this.type = DataSchemaType.parse(type);
    this.length = length;
  }

  private ExtensionSchemaColumn(final ExtensionSchemaColumn original) {
    super(original.newlyGenerated);
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

}
