package edu.harvard.data.io;

import java.io.File;
import java.io.IOException;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

public class S3TableWriter<T extends DataTable> extends TableWriter<T> {

  private final AwsUtils aws;
  private final S3ObjectId fileLocation;

  public S3TableWriter(final Class<T> tableType, final TableFormat format,
      final S3ObjectId fileLocation, final File tmpFile) {
    super(tableType, format, tmpFile);
    this.aws = new AwsUtils();
    this.fileLocation = fileLocation;
  }

  @Override
  public void close() throws IOException {
    super.close();
    aws.putFile(fileLocation, file);
    file.delete();
  }

}
