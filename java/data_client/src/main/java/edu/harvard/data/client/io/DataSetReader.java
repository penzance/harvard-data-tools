package edu.harvard.data.client.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import edu.harvard.data.client.DataTable;
import edu.harvard.data.client.TableFormat;

public interface DataSetReader extends Closeable {

  Map<String, TableReader<? extends DataTable>> getTables() throws IOException;

  <T extends DataTable> TableReader<T> getTable(String tableName, Class<T> tableClass) throws IOException;

  TableFormat getFormat();

  <T extends DataTable> void replaceTable(String tableName, TableReader<T> reader) throws IOException;

}
