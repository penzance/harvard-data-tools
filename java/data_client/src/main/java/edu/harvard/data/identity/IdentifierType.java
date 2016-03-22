package edu.harvard.data.identity;

import java.util.regex.Pattern;

public enum IdentifierType {
  CanvasID (".*"),
  CanvasDataID (".*"),
  HUID ("\\d{8}"),
  XID ("[a-zA-Z][\\d|\\w]{7}"),
  Other (".*");

  private Pattern pattern;
  private IdentifierType(final String regex) {
    pattern = Pattern.compile(regex);
  }

  public Pattern getPattern() {
    return pattern;
  }
}
