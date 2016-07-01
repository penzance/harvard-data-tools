package edu.harvard.data;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;

public class FormatLibrary {
  public enum Format {
    DecompressedCanvasDataFlatFiles("decompressed_canvas"),
    CanvasDataFlatFiles("canvas"),
    DecompressedExcel("decompressed_excel"),
    Excel("excel"),
    Matterhorn("matterhorn"),
    DecompressedMatterhorn("decompressed_matterhorn"),
    CompressedInternal("compressed_internal"),
    DecompressedInternal("decompressed_internal");

    private String label;

    private Format(final String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }

    public static Format fromLabel(final String label) {
      switch (label) {
      case "canvas":
        return CanvasDataFlatFiles;
      case "decompressed_canvas":
        return DecompressedCanvasDataFlatFiles;
      case "excel":
        return Excel;
      case "decompressed_excel":
        return DecompressedExcel;
      case "matterhorn":
        return Matterhorn;
      case "decompressed_matterhorn":
        return DecompressedMatterhorn;
      case "compressed_internal":
        return CompressedInternal;
      case "decompressed_internal":
        return DecompressedInternal;
      default:
        return Format.valueOf(label);
      }
    }
  };

  private final Map<Format, TableFormat> formatMap;

  public FormatLibrary() {
    this.formatMap = new HashMap<Format, TableFormat>();
    formatMap.put(Format.CanvasDataFlatFiles, createCanvasDataFlatFileFormat());
    formatMap.put(Format.DecompressedCanvasDataFlatFiles,
        createDecompressedCanvasDataFlatFileFormat());
    formatMap.put(Format.Excel, createExcelFormat());
    formatMap.put(Format.DecompressedExcel, createDecompressedExcelFormat());
    formatMap.put(Format.Matterhorn, createMatterhornFormat());
    formatMap.put(Format.DecompressedMatterhorn, createDecompressedMatterhornFormat());
    formatMap.put(Format.CompressedInternal, createCompressedInternalFormat());
    formatMap.put(Format.DecompressedInternal, createDecompressedInternalFormat());
  }

  public TableFormat getFormat(final Format format) {
    return formatMap.get(format);
  }

  public static final DateFormat CANVAS_TIMESTAMP_FORMAT = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss");
  public static DateFormat CANVAS_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

  public static DateFormat MATTERHORN_DATE_FORMAT = new SimpleDateFormat(
      "yyyy-MM-dd'T'HH:mm:ssXXX");

  public static final DateFormat LOCAL_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd Z");

  public static final DateFormat JSON_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

  private static final CSVFormat CANVAS_CSV_FORMAT = CSVFormat.TDF.withQuote(null)
      .withNullString("\\N").withRecordSeparator("\n").withIgnoreSurroundingSpaces(false);

  private static final CSVFormat INTERNAL_CSV_FORMAT = CSVFormat.TDF.withQuote('"')
      .withNullString("\\N").withRecordSeparator("\n").withIgnoreSurroundingSpaces(false);

  private static final String CANVAS_FILE_ENCODING = "UTF-8";
  private static final String MATTERHORN_FILE_ENCODING = "UTF-8";

  private TableFormat createCanvasDataFlatFileFormat() {
    final TableFormat canvasFormat = new TableFormat(Format.CanvasDataFlatFiles);
    canvasFormat.setTimestampFormat(CANVAS_TIMESTAMP_FORMAT);
    canvasFormat.setDateFormat(CANVAS_DATE_FORMAT);
    canvasFormat.setIncludeHeaders(false);
    canvasFormat.setEncoding(CANVAS_FILE_ENCODING);
    canvasFormat.setCsvFormat(CANVAS_CSV_FORMAT);
    canvasFormat.setCompression(TableFormat.Compression.Gzip);
    return canvasFormat;
  }

  private TableFormat createDecompressedCanvasDataFlatFileFormat() {
    final TableFormat canvasFormat = new TableFormat(Format.DecompressedCanvasDataFlatFiles);
    canvasFormat.setTimestampFormat(CANVAS_TIMESTAMP_FORMAT);
    canvasFormat.setDateFormat(CANVAS_DATE_FORMAT);
    canvasFormat.setIncludeHeaders(false);
    canvasFormat.setEncoding(CANVAS_FILE_ENCODING);
    canvasFormat.setCsvFormat(CANVAS_CSV_FORMAT);
    canvasFormat.setCompression(TableFormat.Compression.None);
    return canvasFormat;
  }

  private TableFormat createExcelFormat() {
    final TableFormat canvasFormat = new TableFormat(Format.Excel);
    canvasFormat.setTimestampFormat(CANVAS_TIMESTAMP_FORMAT);
    canvasFormat.setDateFormat(CANVAS_DATE_FORMAT);
    canvasFormat.setIncludeHeaders(true);
    canvasFormat.setEncoding(CANVAS_FILE_ENCODING);
    canvasFormat.setCsvFormat(CSVFormat.EXCEL);
    canvasFormat.setCompression(TableFormat.Compression.Gzip);
    return canvasFormat;
  }

  private TableFormat createDecompressedExcelFormat() {
    final TableFormat canvasFormat = new TableFormat(Format.Excel);
    canvasFormat.setTimestampFormat(CANVAS_TIMESTAMP_FORMAT);
    canvasFormat.setDateFormat(CANVAS_DATE_FORMAT);
    canvasFormat.setIncludeHeaders(true);
    canvasFormat.setEncoding(CANVAS_FILE_ENCODING);
    canvasFormat.setCsvFormat(CSVFormat.EXCEL);
    canvasFormat.setCompression(TableFormat.Compression.None);
    return canvasFormat;
  }

  private TableFormat createDecompressedMatterhornFormat() {
    final TableFormat format = new TableFormat(Format.Matterhorn);
    format.setDateFormat(MATTERHORN_DATE_FORMAT);
    format.setTimestampFormat(MATTERHORN_DATE_FORMAT);
    format.setIncludeHeaders(false);
    format.setEncoding(MATTERHORN_FILE_ENCODING);
    format.setCompression(TableFormat.Compression.None);
    return format;
  }

  private TableFormat createMatterhornFormat() {
    final TableFormat format = new TableFormat(Format.Matterhorn);
    format.setDateFormat(MATTERHORN_DATE_FORMAT);
    format.setTimestampFormat(MATTERHORN_DATE_FORMAT);
    format.setIncludeHeaders(false);
    format.setEncoding(MATTERHORN_FILE_ENCODING);
    format.setCompression(TableFormat.Compression.Gzip);
    return format;
  }

  private TableFormat createCompressedInternalFormat() {
    final TableFormat canvasFormat = new TableFormat(Format.CompressedInternal);
    canvasFormat.setTimestampFormat(CANVAS_TIMESTAMP_FORMAT);
    canvasFormat.setDateFormat(CANVAS_DATE_FORMAT);
    canvasFormat.setIncludeHeaders(false);
    canvasFormat.setEncoding(CANVAS_FILE_ENCODING);
    canvasFormat.setCsvFormat(INTERNAL_CSV_FORMAT);
    canvasFormat.setCompression(TableFormat.Compression.Gzip);
    return canvasFormat;
  }

  private TableFormat createDecompressedInternalFormat() {
    final TableFormat canvasFormat = new TableFormat(Format.DecompressedInternal);
    canvasFormat.setTimestampFormat(CANVAS_TIMESTAMP_FORMAT);
    canvasFormat.setDateFormat(CANVAS_DATE_FORMAT);
    canvasFormat.setIncludeHeaders(false);
    canvasFormat.setEncoding(CANVAS_FILE_ENCODING);
    canvasFormat.setCsvFormat(INTERNAL_CSV_FORMAT);
    canvasFormat.setCompression(TableFormat.Compression.None);
    return canvasFormat;
  }


}
