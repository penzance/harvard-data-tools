package edu.harvard.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FullTextTable implements DataTable {

  private final Object id;
  private final String text;

  public FullTextTable(final Object id, final String text) {
    this.id = id;
    this.text = text;
  }

  public Object getId() {
    return id;
  }

  public String getText() {
    return text;
  }

  @Override
  public List<Object> getFieldsAsList(final TableFormat format) {
    final List<Object> fields = new ArrayList<Object>();
    fields.add(id);
    fields.add(text);
    return fields;
  }

  @Override
  public List<String> getFieldNames() {
    final List<String> fields = new ArrayList<String>();
    fields.add("id");
    fields.add("text");
    return fields;
  }

  @Override
  public Map<String, Object> getFieldsAsMap() {
    final Map<String, Object> map = new HashMap<String, Object>();
    map.put("id", id);
    map.put("text", text);
    return map;
  }
}
