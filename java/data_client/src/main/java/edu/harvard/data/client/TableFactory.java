package edu.harvard.data.client;

import java.io.File;
import java.io.IOException;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.client.io.TableReader;
import edu.harvard.data.client.io.TableWriter;

public interface TableFactory {

  TableReader<? extends DataTable> getTableReader(String table, TableFormat format, File file) throws IOException;

  TableReader<? extends DataTable> getTableReader(String table, TableFormat format, AwsUtils aws, S3ObjectId obj, File tempDir) throws IOException;

  TableWriter<? extends DataTable> getTableWriter(String table, TableFormat format, File file) throws IOException;


}
