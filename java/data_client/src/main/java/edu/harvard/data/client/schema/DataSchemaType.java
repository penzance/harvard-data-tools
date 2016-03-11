package edu.harvard.data.client.schema;

import java.util.HashMap;
import java.util.Map;

public enum DataSchemaType {
  BigInt, Boolean, Date, DateTime, DoublePrecision, Integer, Text, Timestamp, VarChar, Guid, Enum;

  private static Map<String, DataSchemaType> stringToType;

  static {
    stringToType = new HashMap<String, DataSchemaType>();
    stringToType.put("bigint", BigInt);
    stringToType.put("boolean", Boolean);
    stringToType.put("date", Date);
    stringToType.put("timestamp without time zone", Date);
    stringToType.put("datetime", DateTime);
    stringToType.put("double precision", DoublePrecision);
    stringToType.put("enum", Enum);
    stringToType.put("int", Integer);
    stringToType.put("integer", Integer);
    stringToType.put("guid", Guid);
    stringToType.put("text", Text);
    stringToType.put("timestamp", Timestamp);
    stringToType.put("varchar", VarChar);
    stringToType.put("character varying", VarChar);
  }

  public static DataSchemaType parse(final String type) {
    final DataSchemaType t = stringToType.get(type);
    if (t == null) {
      return valueOf(type);
    }
    return t;
  }

  public String getHiveType() {
    switch (this) {
    case BigInt:
      return "BIGINT";
    case Boolean:
      return "BOOLEAN";
    case Date:
      return "DATE";
    case DateTime:
    case Timestamp:
      return "TIMESTAMP";
    case DoublePrecision:
      return "DOUBLE";
    case Integer:
      return "INT";
    case Guid:
    case Text:
    case VarChar:
    case Enum:
      return "STRING";
    }
    throw new RuntimeException("Unknown Hive type: " + this);
  }

  public String getRedshiftType() {
    switch (this) {
    case BigInt:
    case Guid:
      return "BIGINT";
    case Boolean:
      return "BOOLEAN";
    case Date:
    case DateTime:
    case Timestamp:
      return "TIMESTAMP";
    case DoublePrecision:
      return "DOUBLE PRECISION";
    case Integer:
      return "INTEGER";
    case Text:
    case VarChar:
    case Enum:
      return "VARCHAR";
    }
    throw new RuntimeException("Unknown Redshift type: " + this);
  }

  public String getRedshiftType(final Integer length) {
    if (this.equals(Guid)) {
      return "VARCHAR(50)";
    }
    String typeString = getRedshiftType();
    if (typeString.equals("VARCHAR")) {
      if (length == null || length == 0) {
        typeString += "(256)";
      } else {
        typeString += "(" + length + ")";
      }
    }
    return typeString;
  }

}
