package edu.harvard.data.client.canvas;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.harvard.data.client.schema.DataSchemaColumn;
import edu.harvard.data.client.schema.DataSchemaType;
import edu.harvard.data.client.schema.SchemaDifference;

public class CanvasDataSchemaColumn extends DataSchemaColumn {

  private final String name;
  private final String description;
  private final String descripton; // Typo shows up in around half the columns.
  private final DataSchemaType type;
  private final CanvasDataSchemaDimension dimension;
  private final int length;
  private final Boolean snowflake; // true for:
  // conversation_message_participant.conversation_message_id,
  // conversation_message_participant.conversation_id,
  // conversation_message_participant.user_id
  private final String seeAlso;
  private final Boolean sortKey; // true for requests.timestamp

  @JsonCreator
  public CanvasDataSchemaColumn(@JsonProperty("name") final String name,
      @JsonProperty("description") final String description,
      @JsonProperty("type") final String type,
      @JsonProperty("dimension") final CanvasDataSchemaDimension dimension,
      @JsonProperty("length") final int length, @JsonProperty("snowflake") final Boolean snowflake,
      @JsonProperty("sortKey") final Boolean sortKey,
      @JsonProperty("descripton") final String descripton,
      @JsonProperty("see_also") final String seeAlso) {
    super(false);
    this.name = cleanColumnName(name);
    this.description = description;
    this.sortKey = sortKey;
    this.descripton = descripton == null ? descripton : description;
    this.type = DataSchemaType.parse(type);
    this.dimension = dimension;
    this.length = length;
    this.snowflake = snowflake;
    this.seeAlso = seeAlso;
  }

  public CanvasDataSchemaColumn(final CanvasDataSchemaColumn original) {
    super(original.newlyGenerated);
    this.name = cleanColumnName(original.name);
    this.description = original.description;
    this.descripton = original.descripton;
    this.type = original.type;
    this.dimension = original.dimension == null ? null : new CanvasDataSchemaDimension(original.dimension);
    this.length = original.length;
    this.snowflake = original.snowflake;
    this.seeAlso = original.seeAlso;
    this.sortKey = original.sortKey;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  public String getDescripton() {
    return descripton;
  }

  @Override
  public DataSchemaType getType() {
    return type;
  }

  public CanvasDataSchemaDimension getDimension() {
    return dimension;
  }

  @Override
  public Integer getLength() {
    return length;
  }

  public Boolean getSnowflake() {
    return snowflake;
  }

  public String getSeeAlso() {
    return seeAlso;
  }

  public Boolean getSortKey() {
    return sortKey;
  }

  public void calculateDifferences(final String tableName, final CanvasDataSchemaColumn column2, final List<SchemaDifference> differences) {
    if (!compareStrings(name, column2.name)) {
      differences.add(new SchemaDifference("name", name, column2.name, tableName, name));
    }
    if (!compareStrings(description, column2.description)) {
      differences.add(new SchemaDifference("description", description, column2.description, tableName, name));
    }
    if (!compareStrings(descripton, column2.descripton)) {
      differences.add(new SchemaDifference("descripton (note spelling)", descripton, column2.descripton, tableName, name));
    }
    if (type != column2.type) {
      differences.add(new SchemaDifference("type", type, column2.type, tableName, name));
    }
    if (length != column2.length) {
      differences.add(new SchemaDifference("length", length, column2.length, tableName, name));
    }
    if (snowflake != column2.snowflake) {
      differences.add(new SchemaDifference("snowflake", snowflake, column2.snowflake, tableName, name));
    }
    if (!compareStrings(seeAlso, column2.seeAlso)) {
      differences.add(new SchemaDifference("seeAlso", seeAlso, column2.seeAlso, tableName, name));
    }
    if (sortKey != column2.sortKey) {
      differences.add(new SchemaDifference("sortKey", sortKey, column2.sortKey, tableName, name));
    }
    if (dimension == null) {
      if (column2.dimension != null) {
        differences.add(new SchemaDifference(tableName + ":" + name + ": Added dimension " + column2.dimension.getName()));
      }
    } else {
      if (column2.dimension == null) {
        differences.add(new SchemaDifference(tableName + ":" + name + ": Removed dimension " + column2.dimension.getName()));
      } else {
        dimension.calculateDifferences(tableName, name, column2.dimension, differences);
      }
    }
  }

  private boolean compareStrings(final String s1, final String s2) {
    if (s1 == null || s2 == null) {
      return s1 == s2;
    }
    return s1.equals(s2);
  }

}
