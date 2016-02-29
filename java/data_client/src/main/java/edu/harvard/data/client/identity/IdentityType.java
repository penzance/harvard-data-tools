package edu.harvard.data.client.identity;

public enum IdentityType {
  RESEARCH_ID (String.class),
  HUID (String.class),
  CANVAS_ID (Long.class),
  CANVAS_DATA_ID (Long.class);

  private Class<?> dataType;

  private IdentityType(final Class<?> dataType) {
    this.dataType = dataType;
  }

  public Class<?> getDataType() {
    return dataType;
  }
}
