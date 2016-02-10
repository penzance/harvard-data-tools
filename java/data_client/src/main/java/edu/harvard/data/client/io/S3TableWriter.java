package edu.harvard.data.client.io;

import java.io.File;
import java.io.IOException;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.client.AwsUtils;
import edu.harvard.data.client.DataTable;
import edu.harvard.data.client.TableFormat;

public class S3TableWriter<T extends DataTable> implements TableWriter<T> {

  private final String tableName;
  private final TableWriter<T> fileWriter;
  private final AwsUtils aws;
  private final S3ObjectId s3Destination;
  private final File tempFile;

  public S3TableWriter(final AwsUtils aws, final Class<T> tableType, final TableFormat format,
      final S3ObjectId s3Destination, final String tableName, final File tempFile) {
    this.tableName = tableName;
    this.aws = aws;
    this.s3Destination = s3Destination;
    this.tempFile = tempFile;
    this.fileWriter = new FileTableWriter<T>(tableType, format, tableName, tempFile);
  }

  @Override
  public void close() throws IOException {
    fileWriter.close();
    aws.getClient().putObject(s3Destination.getBucket(), s3Destination.getKey(), tempFile);
  }

  @Override
  public void add(final DataTable a) throws IOException {
    fileWriter.add(a);
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public void flush() throws IOException {
    fileWriter.flush();
  }

  @Override
  public void pipe(final TableReader<T> in) throws IOException {
    fileWriter.pipe(in);
  }

}
