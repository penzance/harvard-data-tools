package edu.harvard.data.io;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.UUID;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataTable;
import edu.harvard.data.TableFormat;

/**
 * {@link TableReader} implementation that reads records from a delimited data
 * file.
 * <P>
 * This class is mostly a wrapper around a {@link FileTableReader}. When a
 * client first attempts to retrieve the iterator for the data table, this class
 * downloads the data file from S3 to a temporary location on the local file
 * system. It then delegates to {@code FileTableReader} to read the file.
 * <P>
 * This class is not thread-safe. Any access synchronization must be performed
 * by the caller.
 *
 * @param <T>
 *          the record type that this file reader parses.
 */
public class S3TableReader<T extends DataTable> implements TableReader<T> {

  FileTableReader<T> reader;
  private final S3ObjectId obj;
  private final TableFormat format;
  private final Class<T> tableType;
  private final File tempFile;
  private final AwsUtils aws;

  /**
   * Create a new reader for a file on S3.
   *
   * @param aws
   *          an instance of the {@link AwsUtils} class used to abstract access
   *          to Amazon Web Services.
   * @param tableType
   *          a reference to the template class {@code T} that will be used to
   *          create new records.
   * @param format
   *          the {@link TableFormat} that indicates how the data file is
   *          formatted.
   * @param obj
   *          an {S3ObjectId} instance that contains the bucket and key used to
   *          address the file on S3.
   * @param tempDir
   *          a {@link File} pointing to a scratch directory where a temporary
   *          file can be created. The implementation will make a best-effort
   *          attempt to clean up any files stored in the temporary directory,
   *          but in some circumstances (such as a client code failing to call
   *          {@link #close} or a VM crash) there may be some files left over in
   *          this directory.
   */
  public S3TableReader(final AwsUtils aws, final Class<T> tableType, final TableFormat format,
      final S3ObjectId obj, final File tempDir) {
    this.aws = aws;
    this.tableType = tableType;
    this.format = format;
    this.obj = obj;
    this.tempFile = new File(tempDir, UUID.randomUUID().toString());
  }

  /**
   * Fetch the file from S3 and create a {@link FileTableReader} instance to
   * which this reader will delegate. This call is idempotent; multiple calls
   * will always return the same reader instance.
   *
   * @return a {@code FileTableReader} initialized to point to the file
   *         temporarily fetched from S3.
   *
   * @throws IOException
   *           if an error occurs when downloading the file or creating the
   *           {@code FileTableReader}.
   */
  private FileTableReader<T> getReader() throws IOException {
    if (reader == null) {
      if (tempFile.exists()) {
        tempFile.delete();
      }
      tempFile.getParentFile().mkdirs();
      aws.getFile(obj, tempFile);
      reader = new FileTableReader<T>(tableType, format, tempFile);
    }
    return reader;
  }

  @Override
  public Iterator<T> iterator() {
    try {
      return getReader().iterator();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      tempFile.delete();
    }
    reader = null;
  }

}
