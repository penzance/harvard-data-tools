package edu.harvard.data.client;

import java.io.File;

import org.apache.commons.csv.CSVRecord;

public class RecordParsingException extends RuntimeException {

  private static final long serialVersionUID = 1L;
  private final CSVRecord record;
  private final File file;
  private final int line;

  public RecordParsingException(final File file, final int line, final CSVRecord record, final Throwable error) {
    super(error);
    this.record = record;
    this.file = file;
    this.line = line;
  }

  public CSVRecord getRecord() {
    return record;
  }

  public File getFile() {
    return file;
  }

  @Override
  public String getMessage() {
    return "Parse error in file " + file + ":" + line + ":\n    " + record.toString();
  }

}
