package edu.harvard.data;

import java.util.List;
import java.util.Map;

public interface DataTable {

  // Date and timestamp should be formatted to string. Everything else stays as-is.
  List<Object> getFieldsAsList(final TableFormat format);
  List<String> getFieldNames();
  Map<String, Object> getFieldsAsMap();

}
