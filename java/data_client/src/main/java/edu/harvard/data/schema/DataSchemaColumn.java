package edu.harvard.data.schema;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class DataSchemaColumn {

  @JsonIgnore
  protected boolean newlyGenerated;

  public abstract String getName();

  public abstract String getSourceName();

  public abstract String getDescription();

  public abstract DataSchemaType getType();

  public abstract Integer getLength();

  public abstract DataSchemaColumn copy();

  protected DataSchemaColumn(final boolean newlyGenerated) {
    this.newlyGenerated = newlyGenerated;
  }

  public boolean getNewlyGenerated() {
    return newlyGenerated;
  }

  public void setNewlyGenerated(final boolean newlyGenerated) {
    this.newlyGenerated = newlyGenerated;
  }

  protected String cleanColumnName(final String name) {
    if (name == null) {
      return null;
    }
    final Set<String> badChars = new HashSet<String>();
    badChars.add("@");
    String clean = name;
    while(badChars.contains(clean.substring(0, 1))) {
      clean = clean.substring(1);
    }
    switch(name) {
    case "default":
      return "is_default";
    default:
      return clean;
    }
  }

}
