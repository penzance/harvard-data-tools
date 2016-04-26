package edu.harvard.data.identity;

import java.util.regex.Pattern;

public enum IdentifierType {
  ResearchUUID ("research_id", "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}", String.class),
  HUID ("huid", "\\d{8}", String.class),
  XID ("xid", "[a-zA-Z][\\d|\\w]{7}", String.class),
  CanvasID ("canvas_id", "", Long.class),
  CanvasDataID ("canvas_data_id", "", Long.class),
  Other ("other", "", Void.class);

  private String fieldName;
  private Pattern pattern;
  private Class<?> type;
  private IdentifierType(final String fieldName, final String regex, final Class<?> type) {
    this.fieldName = fieldName;
    pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    this.type = type;
  }

  public String getFieldName() {
    return fieldName;
  }

  public Pattern getPattern() {
    if (!type.equals(String.class)) {
      throw new RuntimeException("Can't access regex pattern for an identifier of type " + type);
    }
    return pattern;
  }

  public Class<?> getType() {
    return type;
  }
}
