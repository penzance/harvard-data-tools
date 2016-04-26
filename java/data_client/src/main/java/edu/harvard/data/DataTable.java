package edu.harvard.data;

import java.util.List;
import java.util.Map;

public interface DataTable {

  List<Object> getFieldsAsList(final TableFormat format);
  List<String> getFieldNames();
  Map<String, Object> getFieldsAsMap();

}
