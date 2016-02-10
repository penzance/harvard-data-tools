package edu.harvard.data.client.canvas.api;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CanvasDataSchemaDimension {

  private final String name;
  private final String id;
  private final String role;

  @JsonCreator
  public CanvasDataSchemaDimension(@JsonProperty("name") final String name,
      @JsonProperty("id") final String id, @JsonProperty("role") final String role) {
    this.name = name;
    this.id = id;
    this.role = role;
  }

  public CanvasDataSchemaDimension(final CanvasDataSchemaDimension original) {
    this.name = original.name;
    this.id = original.id;
    this.role = original.role;
  }

  public String getName() {
    return name;
  }

  public String getId() {
    return id;
  }

  public String getRole() {
    return role;
  }

  public void calculateDifferences(final String tableName, final String columnName,
      final CanvasDataSchemaDimension dimension2, final List<SchemaDifference> differences) {
    if (!compareStrings(name, dimension2.name)) {
      differences.add(new SchemaDifference("name", name, dimension2.name, tableName, columnName));
    }
    if (!compareStrings(id, dimension2.id)) {
      differences.add(new SchemaDifference("id", id, dimension2.id, tableName, columnName));
    }
    if (!compareStrings(role, dimension2.role)) {
      differences.add(new SchemaDifference("role", role, dimension2.role, tableName, columnName));
    }
  }

  private boolean compareStrings(final String s1, final String s2) {
    if (s1 == null || s2 == null) {
      return s1 == s2;
    }
    return s1.equals(s2);
  }

}
