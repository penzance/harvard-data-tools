package edu.harvard.data.identity;

import java.util.regex.Pattern;

public enum IdentifierType {
  ResearchUUID ("research_id", ".*", String.class),
  HUID ("huid", "\\d{8}", String.class),
  XID ("xid", "[a-zA-Z][\\d|\\w]{7}", String.class),
  CanvasID ("canvas_id", ".*", Long.class),
  CanvasDataID ("canvas_data_id", ".*", Long.class),
  Other ("other", ".*", Void.class);

  private String fieldName;
  private Pattern pattern;
  private Class<?> type;
  private IdentifierType(final String fieldName, final String regex, final Class<?> type) {
    this.fieldName = fieldName;
    pattern = Pattern.compile(regex);
    this.type = type;
  }

  public String getFieldName() {
    return fieldName;
  }

  public Pattern getPattern() {
    return pattern;
  }

  public Class<?> getType() {
    return type;
  }
}
