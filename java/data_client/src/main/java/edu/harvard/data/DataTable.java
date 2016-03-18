package edu.harvard.data;

import java.util.List;

public interface DataTable {

  List<Object> getFieldsAsList(final TableFormat format);

}
