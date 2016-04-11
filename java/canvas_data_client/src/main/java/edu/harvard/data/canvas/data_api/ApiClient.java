package edu.harvard.data.canvas.data_api;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.schema.DataSchema;
import edu.harvard.data.schema.UnexpectedApiResponseException;

public class ApiClient {
  private final RestUtils rest;
  private final TypeFactory typeFactory;

  public ApiClient(final String host, final String key, final String secret) {
    this.rest = new RestUtils(host, key, secret);
    this.typeFactory = new ObjectMapper().getTypeFactory();
  }

  public List<DataDump> getDumps()
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    final CollectionType type = typeFactory.constructCollectionType(List.class,
        DataDump.class);
    final List<DataDump> dumps = rest.makeApiCall("/api/account/self/dump", 200, type);
    for (final DataDump dump : dumps) {
      dump.setRestUtils(rest);
    }
    return dumps;
  }

  public DataDump getLatestDump()
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    final JavaType type = typeFactory.constructType(DataDump.class);
    final DataDump dump = rest.makeApiCall("/api/account/self/file/latest", 200, type);
    dump.setRestUtils(rest);
    return dump;
  }

  public DataDump getDump(final String id)
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    final JavaType type = typeFactory.constructType(DataDump.class);
    final DataDump dump = rest.makeApiCall("/api/account/self/file/byDump/" + id, 200, type);
    dump.setRestUtils(rest);
    return dump;
  }

  public DataDump getDump(final long sequence)
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    for (final DataDump dump : getDumps()) {
      if (dump.getSequence() == sequence) {
        return getDump(dump.getDumpId());
      }
    }
    return null;
  }

  public CanvasDataTableHistory getTableHistory(final String table)
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    final JavaType type = typeFactory.constructType(CanvasDataTableHistory.class);
    final CanvasDataTableHistory history = rest
        .makeApiCall("/api/account/self/file/byTable/" + table, 200, type);
    history.setRestUtils(rest);
    return history;
  }

  public DataSchema getSchema(final String version)
      throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    final JavaType type = typeFactory.constructType(CanvasDataSchema.class);
    final CanvasDataSchema schema = rest.makeApiCall("/api/schema/" + version, 200, type);
    return schema;
  }

  public List<CanvasDataSchemaSummary> getSchemas() throws DataConfigurationException, UnexpectedApiResponseException, IOException {
    final CollectionType type = typeFactory.constructCollectionType(List.class,
        CanvasDataSchemaSummary.class);
    final List<CanvasDataSchemaSummary> schemas = rest.makeApiCall("/api/schema", 200, type);
    return schemas;
  }
}
