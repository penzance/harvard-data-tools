package edu.harvard.data.client;

import java.util.List;

public interface DataTable {

  List<Object> getFieldsAsList(final TableFormat formatter);

}
