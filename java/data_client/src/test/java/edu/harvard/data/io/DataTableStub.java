package edu.harvard.data.io;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;

import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

class DataTableStub implements DataTable {

  static final List<String> fieldNames;

  static {
    fieldNames = new ArrayList<String>();
    fieldNames.add("int_1");
    fieldNames.add("string_1");
    fieldNames.add("string_2");
    fieldNames.add("timestamp");
    fieldNames.add("date");
  }

  public static List<DataTableStub> generateRecords(final int count, final TableFormat format) {
    final DataTableStub base = new DataTableStub(format);
    final List<DataTableStub> lst = new ArrayList<DataTableStub>();
    for (int i=0; i<count; i++) {
      lst.add(new DataTableStub(format, i, "" + i, "123", base.timestamp, base.date));
    }
    return lst;
  }

  Integer int1;
  String string1;
  String string2;
  Timestamp timestamp;
  Date date;
  TableFormat format;

  public DataTableStub() {
  }

  public DataTableStub(final TableFormat format, final CSVRecord record) throws ParseException {
    final String $int1 = record.get(0);
    if ($int1 != null && $int1.length() > 0) {
      this.int1 = Integer.valueOf($int1);
    }
    this.string1 = record.get(1);
    this.string2 = record.get(2);
    final String timestamp = record.get(3);
    if (timestamp != null && timestamp.length() > 0) {
      this.timestamp = Timestamp.valueOf(timestamp);
    }
    final String date = record.get(4);
    if (date != null && date.length() > 0) {
      this.date = format.getDateFormat().parse(date);
    }
  }


  public DataTableStub(final TableFormat format) {
    this.format = format;
    int1 = 42;
    string1 = "String Value";
    string2 = "123";
    timestamp = new Timestamp(
        new GregorianCalendar(2016, Calendar.MARCH, 13, 2, 30).getTimeInMillis());
    date = new GregorianCalendar(2016, Calendar.FEBRUARY, 29).getTime();
  }

  public DataTableStub(final TableFormat format, final int i, final String s1, final String s2,
      final Timestamp ts, final Date d) {
    this(format);
    this.int1 = i;
    this.string1 = s1;
    this.string2 = s2;
    this.timestamp = ts;
    this.date = d;
  }

  @Override
  public List<Object> getFieldsAsList(final TableFormat format) {
    final List<Object> lst = new ArrayList<Object>();
    lst.add(int1);
    lst.add(string1);
    lst.add(string2);
    lst.add(format.formatTimestamp(timestamp));
    lst.add(format.formatTimestamp(date));
    return lst;
  }

  @Override
  public List<String> getFieldNames() {
    return fieldNames;
  }

  @Override
  public Map<String, Object> getFieldsAsMap() {
    final Map<String, Object> map = new HashMap<String, Object>();
    map.put("int_1", int1);
    map.put("string_1", string1);
    map.put("string_2", string2);
    map.put("timestamp", timestamp);
    map.put("date", date);
    return map;
  }

  public String recordString() {
    String s = "";
    s += int1 == null ? "\\N\t" : int1 + "\t";
    s += string1 == null ? "\\N\t" : string1 + "\t";
    s += string2 == null ? "\\N\t" : string2 + "\t";
    s += timestamp == null ? "\\N\t" : format.formatTimestamp(timestamp) + "\t";
    s += date == null ? "\\N" : format.formatTimestamp(date);
    return s;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((date == null) ? 0 : date.hashCode());
    result = prime * result + ((format == null) ? 0 : format.hashCode());
    result = prime * result + ((int1 == null) ? 0 : int1.hashCode());
    result = prime * result + ((string1 == null) ? 0 : string1.hashCode());
    result = prime * result + ((string2 == null) ? 0 : string2.hashCode());
    result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final DataTableStub other = (DataTableStub) obj;
    if (date == null) {
      if (other.date != null) {
        return false;
      }
    } else if (!date.equals(other.date)) {
      return false;
    }
    if (int1 == null) {
      if (other.int1 != null) {
        return false;
      }
    } else if (!int1.equals(other.int1)) {
      return false;
    }
    if (string1 == null) {
      if (other.string1 != null) {
        return false;
      }
    } else if (!string1.equals(other.string1)) {
      return false;
    }
    if (string2 == null) {
      if (other.string2 != null) {
        return false;
      }
    } else if (!string2.equals(other.string2)) {
      return false;
    }
    if (timestamp == null) {
      if (other.timestamp != null) {
        return false;
      }
    } else if (!timestamp.equals(other.timestamp)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return getFieldsAsMap().toString();
  }

  static String headerString() {
    return "int_1\tstring_1\tstring_2\ttimestamp\tdate";
  }

}