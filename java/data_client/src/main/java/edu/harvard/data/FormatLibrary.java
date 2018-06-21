package edu.harvard.data;

import java.text.SimpleDateFormat;

import org.apache.commons.csv.CSVFormat;

public class FormatLibrary {
  public enum Format {
    DecompressedCanvasDataFlatFiles("decompressed_canvas"), CanvasDataFlatFiles(
        "canvas"), DecompressedExcel("decompressed_excel"), Excel("excel"), Matterhorn(
            "matterhorn"), DecompressedMatterhorn("decompressed_matterhorn"), CompressedInternal(
                "compressed_internal"), DecompressedInternal("decompressed_internal"), Mediasites(
                    "mediasites"), DecompressedMediasites("decompressed_mediasites"), Sis(
                    		"sis"), DecompressedSis("decompressed_sis");

    private final String label;

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
      case "mediasites":
    	return Mediasites;
      case "decompressed_mediasites":
    	return DecompressedMediasites;
      case "sis":
    	return Sis;
      case "decompressed_sis":
    	return DecompressedSis;    	
      case "compressed_internal":
        return CompressedInternal;
      case "decompressed_internal":
        return DecompressedInternal;
      default:
        return Format.valueOf(label);
      }
    }
  };

  public TableFormat getFormat(final Format format) {
    switch (format) {
    case CanvasDataFlatFiles:
      return createCanvasDataFlatFileFormat();
    case DecompressedCanvasDataFlatFiles:
      return createDecompressedCanvasDataFlatFileFormat();
    case Excel:
      return createExcelFormat();
    case DecompressedExcel:
      return createDecompressedExcelFormat();
    case Matterhorn:
      return createMatterhornFormat();
    case DecompressedMatterhorn:
      return createDecompressedMatterhornFormat();
    case Mediasites:
      return createMediasitesFormat();
    case DecompressedMediasites:
      return createDecompressedMediasitesFormat();
    case Sis:
      return createSisFormat();
    case DecompressedSis:
      return createDecompressedSisFormat();
    case CompressedInternal:
      return createCompressedInternalFormat();
    case DecompressedInternal:
      return createDecompressedInternalFormat();
    default:
      throw new RuntimeException("Unknown format " + format);
    }
  }

  public static final String CANVAS_TIMESTAMP_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss.SSS";
  public static final String CANVAS_DATE_FORMAT_STRING = "yyyy-MM-dd";
  public static final String MATTERHORN_DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssXXX";
  public static final String MEDIASITES_TIMESTAMP_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssXXXZ";
  public static final String MEDIASITES_DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss";
  public static final String LOCAL_DATE_FORMAT_STRING = "yyyy-MM-dd Z";
  public static final String JSON_DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssZ";

  private static final CSVFormat CANVAS_CSV_FORMAT = CSVFormat.TDF.withQuote(null)
      .withNullString("\\N").withRecordSeparator("\n").withIgnoreSurroundingSpaces(false);

  private static final CSVFormat INTERNAL_CSV_FORMAT = CSVFormat.TDF.withQuote(null).withEscape('/')
      .withNullString("\\N").withRecordSeparator("\n").withIgnoreSurroundingSpaces(false);

  private static final CSVFormat INTERNAL_SIS_FORMAT = CSVFormat.TDF.withQuote(null).withEscape('/')
	      .withNullString("null").withRecordSeparator("\n").withIgnoreSurroundingSpaces(false);  
  
  private static final String CANVAS_FILE_ENCODING = "UTF-8";
  private static final String MATTERHORN_FILE_ENCODING = "UTF-8";
  private static final String MEDIASITES_FILE_ENCODING = "UTF-8";

  private TableFormat createCanvasDataFlatFileFormat() {
    final TableFormat canvasFormat = new TableFormat(Format.CanvasDataFlatFiles);
    canvasFormat.setTimestampFormat(new SimpleDateFormat(CANVAS_TIMESTAMP_FORMAT_STRING));
    canvasFormat.setDateFormat(new SimpleDateFormat(CANVAS_DATE_FORMAT_STRING));
    canvasFormat.setIncludeHeaders(false);
    canvasFormat.setEncoding(CANVAS_FILE_ENCODING);
    canvasFormat.setCsvFormat(CANVAS_CSV_FORMAT);
    canvasFormat.setCompression(TableFormat.Compression.Gzip);
    return canvasFormat;
  }

  private TableFormat createDecompressedCanvasDataFlatFileFormat() {
    final TableFormat canvasFormat = new TableFormat(Format.DecompressedCanvasDataFlatFiles);
    canvasFormat.setTimestampFormat(new SimpleDateFormat(CANVAS_TIMESTAMP_FORMAT_STRING));
    canvasFormat.setDateFormat(new SimpleDateFormat(CANVAS_DATE_FORMAT_STRING));
    canvasFormat.setIncludeHeaders(false);
    canvasFormat.setEncoding(CANVAS_FILE_ENCODING);
    canvasFormat.setCsvFormat(CANVAS_CSV_FORMAT);
    canvasFormat.setCompression(TableFormat.Compression.None);
    return canvasFormat;
  }

  private TableFormat createExcelFormat() {
    final TableFormat canvasFormat = new TableFormat(Format.Excel);
    canvasFormat.setTimestampFormat(new SimpleDateFormat(CANVAS_TIMESTAMP_FORMAT_STRING));
    canvasFormat.setDateFormat(new SimpleDateFormat(CANVAS_DATE_FORMAT_STRING));
    canvasFormat.setIncludeHeaders(true);
    canvasFormat.setEncoding(CANVAS_FILE_ENCODING);
    canvasFormat.setCsvFormat(CSVFormat.EXCEL);
    canvasFormat.setCompression(TableFormat.Compression.Gzip);
    return canvasFormat;
  }

  private TableFormat createDecompressedExcelFormat() {
    final TableFormat canvasFormat = new TableFormat(Format.Excel);
    canvasFormat.setTimestampFormat(new SimpleDateFormat(CANVAS_TIMESTAMP_FORMAT_STRING));
    canvasFormat.setDateFormat(new SimpleDateFormat(CANVAS_DATE_FORMAT_STRING));
    canvasFormat.setIncludeHeaders(true);
    canvasFormat.setEncoding(CANVAS_FILE_ENCODING);
    canvasFormat.setCsvFormat(CSVFormat.EXCEL);
    canvasFormat.setCompression(TableFormat.Compression.None);
    return canvasFormat;
  }

  private TableFormat createDecompressedMatterhornFormat() {
    final TableFormat format = new TableFormat(Format.Matterhorn);
    format.setDateFormat(new SimpleDateFormat(MATTERHORN_DATE_FORMAT_STRING));
    format.setTimestampFormat(new SimpleDateFormat(MATTERHORN_DATE_FORMAT_STRING));
    format.setIncludeHeaders(false);
    format.setEncoding(MATTERHORN_FILE_ENCODING);
    format.setCompression(TableFormat.Compression.None);
    return format;
  }

  private TableFormat createMatterhornFormat() {
    final TableFormat format = new TableFormat(Format.Matterhorn);
    format.setDateFormat(new SimpleDateFormat(MATTERHORN_DATE_FORMAT_STRING));
    format.setTimestampFormat(new SimpleDateFormat(MATTERHORN_DATE_FORMAT_STRING));
    format.setIncludeHeaders(false);
    format.setEncoding(MATTERHORN_FILE_ENCODING);
    format.setCompression(TableFormat.Compression.Gzip);
    return format;
  }
  
  private TableFormat createDecompressedMediasitesFormat() {
	final TableFormat format = new TableFormat(Format.Mediasites);
	format.setDateFormat(new SimpleDateFormat(MEDIASITES_DATE_FORMAT_STRING));
	format.setTimestampFormat(new SimpleDateFormat(MEDIASITES_TIMESTAMP_FORMAT_STRING));
	format.setIncludeHeaders(false);
	format.setEncoding(MEDIASITES_FILE_ENCODING);
	format.setCompression(TableFormat.Compression.None);
	return format;
  }

  private TableFormat createMediasitesFormat() {
	final TableFormat format = new TableFormat(Format.Mediasites);
	format.setDateFormat(new SimpleDateFormat(MEDIASITES_DATE_FORMAT_STRING));
	format.setTimestampFormat(new SimpleDateFormat(MEDIASITES_TIMESTAMP_FORMAT_STRING));
	format.setIncludeHeaders(false);
	format.setEncoding(MEDIASITES_FILE_ENCODING);
	format.setCompression(TableFormat.Compression.Gzip);
	return format;
  }

  private TableFormat createDecompressedSisFormat() {
	final TableFormat format = new TableFormat(Format.DecompressedInternal);
	format.setTimestampFormat(new SimpleDateFormat(CANVAS_TIMESTAMP_FORMAT_STRING));
	format.setDateFormat(new SimpleDateFormat(CANVAS_DATE_FORMAT_STRING));
	format.setIncludeHeaders(false);
	format.setEncoding(CANVAS_FILE_ENCODING);
	format.setCsvFormat(INTERNAL_SIS_FORMAT);
	format.setCompression(TableFormat.Compression.None);
	return format;
  }

  private TableFormat createSisFormat() {
	final TableFormat format = new TableFormat(Format.Mediasites);
	format.setDateFormat(new SimpleDateFormat(MEDIASITES_DATE_FORMAT_STRING));
	format.setTimestampFormat(new SimpleDateFormat(MEDIASITES_TIMESTAMP_FORMAT_STRING));
	format.setIncludeHeaders(false);
	format.setEncoding(MEDIASITES_FILE_ENCODING);
	format.setCompression(TableFormat.Compression.Gzip);
	return format;
  }
  
  
  private TableFormat createCompressedInternalFormat() {
    final TableFormat format = new TableFormat(Format.CompressedInternal);
    format.setTimestampFormat(new SimpleDateFormat(CANVAS_TIMESTAMP_FORMAT_STRING));
    format.setDateFormat(new SimpleDateFormat(CANVAS_DATE_FORMAT_STRING));
    format.setIncludeHeaders(false);
    format.setEncoding(CANVAS_FILE_ENCODING);
    format.setCsvFormat(INTERNAL_CSV_FORMAT);
    format.setCompression(TableFormat.Compression.Gzip);
    return format;
  }

  private TableFormat createDecompressedInternalFormat() {
    final TableFormat format = new TableFormat(Format.DecompressedInternal);
    format.setTimestampFormat(new SimpleDateFormat(CANVAS_TIMESTAMP_FORMAT_STRING));
    format.setDateFormat(new SimpleDateFormat(CANVAS_DATE_FORMAT_STRING));
    format.setIncludeHeaders(false);
    format.setEncoding(CANVAS_FILE_ENCODING);
    format.setCsvFormat(INTERNAL_CSV_FORMAT);
    format.setCompression(TableFormat.Compression.None);
    return format;
  }

}
