package edu.harvard.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.FormatLibrary.Format;

public class TableFormat {

  public enum Compression { None, Gzip };

  private DateFormat timestampFormat;
  private DateFormat dateFormat;
  private boolean includeHeaders;
  private String encoding;
  private CSVFormat csvFormat;
  private Compression compression;
  private final Format format;
  private ObjectMapper jsonMapper;

  public TableFormat(final FormatLibrary.Format format) {
    this.format = format;
    this.timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
    this.dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    this.includeHeaders = true;
    this.encoding = "UTF-8";
    this.csvFormat = CSVFormat.DEFAULT;
    this.compression = Compression.None;
    this.jsonMapper = new ObjectMapper();
  }

  public DateFormat getTimstampFormat() {
    return timestampFormat;
  }

  public DateFormat getDateFormat() {
    return dateFormat;
  }

  public String getEncoding() {
    return encoding;
  }

  public CSVFormat getCsvFormat() {
    return csvFormat;
  }

  public boolean includeHeaders() {
    return includeHeaders;
  }

  public Compression getCompression() {
    return compression;
  }

  public void setTimestampFormat(final DateFormat timestampFormat) {
    this.timestampFormat = timestampFormat;
  }

  public void setDateFormat(final DateFormat dateFormat) {
    this.dateFormat = dateFormat;
  }

  public void setIncludeHeaders(final boolean includeHeaders) {
    this.includeHeaders = includeHeaders;
  }

  public void setEncoding(final String encoding) {
    this.encoding = encoding;
  }

  public void setCsvFormat(final CSVFormat csvFormat) {
    this.csvFormat = csvFormat;
  }

  public void setCompression(final Compression compression) {
    this.compression = compression;
  }

  public ObjectMapper getJsonMapper() {
    return jsonMapper;
  }

  public void setJsonMapper(final ObjectMapper jsonMapper) {
    this.jsonMapper = jsonMapper;
  }

  public String formatTimestamp(final Date date) {
    if (date == null) {
      return null;
    }
    return dateFormat.format(date);
  }

  public String formatTimestamp(final Timestamp time) {
    if (time == null) {
      return null;
    }
    final String timestamp = timestampFormat.format(time).toString();
    return cleanTimestampString(timestamp);
  }

  public String cleanTimestampString(String ts) {
    if (ts.endsWith(".000Z")) {
      ts = ts.substring(0, ts.lastIndexOf(".")) + "Z";
    }
    if (ts.endsWith(".0")) {
      ts = ts.substring(0, ts.lastIndexOf("."));
    }
    return ts;
  }


  public Format getFormat() {
    return format;
  }

  public String getExtension() {
    if (compression == Compression.Gzip) {
      return ".gz";
    }
    if (csvFormat.getDelimiter() == '\t') {
      return ".tsv";
    }
    if (csvFormat.getDelimiter() == ',') {
      return ".csv";
    }
    return "";
  }

  public OutputStream getOutputStream(final File file) throws IOException {
    switch (compression) {
    case Gzip:
      return new GZIPOutputStream(new FileOutputStream(file));
    case None:
      return new FileOutputStream(file);
    default:
      throw new RuntimeException("Unknown compression: " + compression);
    }
  }

  public InputStream getInputStream(final File file) throws FileNotFoundException, IOException {
    switch (getCompression()) {
    case Gzip:
      return new GZIPInputStream(new FileInputStream(file));
    case None:
      return new FileInputStream(file);
    default:
      throw new RuntimeException("Unknown compression format: " + getCompression());
    }
  }
}
