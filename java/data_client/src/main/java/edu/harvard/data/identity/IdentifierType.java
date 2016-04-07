package edu.harvard.data.identity;

import java.util.regex.Pattern;

public enum IdentifierType {
  CanvasID (".*", Long.class),
  CanvasDataID (".*", Long.class),
  HUID ("\\d{8}", String.class),
  XID ("[a-zA-Z][\\d|\\w]{7}", String.class),
  Other (".*", Void.class);

  private Pattern pattern;
  private Class<?> type;
  private IdentifierType(final String regex, final Class<?> type) {
    pattern = Pattern.compile(regex);
    this.type = type;
  }

  public Pattern getPattern() {
    return pattern;
  }

  public Class<?> getType() {
    return type;
  }
}
