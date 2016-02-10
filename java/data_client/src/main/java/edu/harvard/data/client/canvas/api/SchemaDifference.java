package edu.harvard.data.client.canvas.api;

public class SchemaDifference {

  private String msg;
  private String[] location;
  private String field;
  private Object oldVersion;
  private Object newVersion;

  public SchemaDifference(final String field, final Object oldVersion, final Object newVersion,
      final String... location) {
    this.field = field;
    this.oldVersion = oldVersion;
    this.newVersion = newVersion;
    this.location = location;
  }

  public SchemaDifference(final String msg) {
    this.msg = msg;
  }

  public String getField() {
    return field;
  }

  @Override
  public String toString() {
    if (msg != null) {
      return msg;
    }
    String loc = "";
    for (final String l : location) {
      loc += l + ":";
    }
    return loc + field + " changed\n   was: " + oldVersion + "\n   now: " + newVersion;
  }
}
