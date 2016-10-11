// This file was generated automatically. Do not edit.

package edu.harvard.data.matterhorn.togenerate;

import java.io.File;
import java.io.IOException;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataTable;
import edu.harvard.data.TableFactory;
import edu.harvard.data.TableFormat;
import edu.harvard.data.io.FileTableReader;
import edu.harvard.data.io.S3TableReader;
import edu.harvard.data.io.S3TableWriter;
import edu.harvard.data.io.TableReader;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.matterhorn.bindings.output.OutputEvent;
import edu.harvard.data.matterhorn.bindings.output.OutputGeoIp;
import edu.harvard.data.matterhorn.bindings.output.OutputVideo;

public class ToGenerateOutputMatterhornTableFactory implements TableFactory {

  @Override
  public TableReader<? extends DataTable> getTableReader(final String table, final TableFormat format, final File file) throws IOException {
    switch(table) {
    case "event":
      return new FileTableReader<OutputEvent>(OutputEvent.class, format, file);
    case "geo_ip":
      return new FileTableReader<OutputGeoIp>(OutputGeoIp.class, format, file);
    case "video":
      return new FileTableReader<OutputVideo>(OutputVideo.class, format, file);
    }
    return null;
  }

  @Override
  public TableReader<? extends DataTable> getTableReader(final String table, final TableFormat format, final AwsUtils aws, final S3ObjectId obj, final File tempDir) throws IOException {
    switch(table) {
    case "event":
      return new S3TableReader<OutputEvent>(aws, OutputEvent.class, format, obj, tempDir);
    case "geo_ip":
      return new S3TableReader<OutputGeoIp>(aws, OutputGeoIp.class, format, obj, tempDir);
    case "video":
      return new S3TableReader<OutputVideo>(aws, OutputVideo.class, format, obj, tempDir);
    }
    return null;
  }

  @Override
  public TableWriter<? extends DataTable> getTableWriter(final String table, final TableFormat format, final File file) throws IOException {
    switch(table) {
    case "event":
      return new TableWriter<OutputEvent>(OutputEvent.class, format, file);
    case "geo_ip":
      return new TableWriter<OutputGeoIp>(OutputGeoIp.class, format, file);
    case "video":
      return new TableWriter<OutputVideo>(OutputVideo.class, format, file);
    }
    return null;
  }

  public TableWriter<? extends DataTable> getTableWriter(final String tableName, final TableFormat format,
      final S3ObjectId outputLocation, final File tempFile) {
    switch(tableName) {
    case "event":
      return new S3TableWriter<OutputEvent>(OutputEvent.class, format, outputLocation, tempFile);
    case "geo_ip":
      return new S3TableWriter<OutputGeoIp>(OutputGeoIp.class, format, outputLocation, tempFile);
    case "video":
      return new S3TableWriter<OutputVideo>(OutputVideo.class, format, outputLocation, tempFile);
    }
    return null;
  }
}
