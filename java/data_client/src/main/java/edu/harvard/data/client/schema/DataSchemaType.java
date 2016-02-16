package edu.harvard.data.client.schema;

import java.util.HashMap;
import java.util.Map;

public enum DataSchemaType {
  BigInt, Boolean, Date, DateTime, DoublePrecision, Int, Integer, Text, Timestamp, VarChar;

  private static Map<String, DataSchemaType> stringToType;

  static {
    stringToType = new HashMap<String, DataSchemaType>();
    stringToType.put("bigint", BigInt);
    stringToType.put("boolean", Boolean);
    stringToType.put("date", Date);
    stringToType.put("datetime", DateTime);
    stringToType.put("double precision", DoublePrecision);
    stringToType.put("int", Int);
    stringToType.put("integer", Integer);
    stringToType.put("text", Text);
    stringToType.put("timestamp", Timestamp);
    stringToType.put("varchar", VarChar);
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
    case Int:
    case Integer:
      return "INT";
    case Text:
    case VarChar:
      return "STRING";
    default:
      throw new RuntimeException("Unknown Hive type: " + this);
    }
  }

  public String getRedshiftType() {
    switch (this) {
    case BigInt:
      return "BIGINT";
    case Boolean:
      return "BOOLEAN";
    case Date:
    case DateTime:
    case Timestamp:
      return "TIMESTAMP";
    case DoublePrecision:
      return "DOUBLE PRECISION";
    case Int:
    case Integer:
      return "INTEGER";
    case Text:
    case VarChar:
      return "VARCHAR";
    default:
      throw new RuntimeException("Unknown Redshift type: " + this);
    }
  }

}
