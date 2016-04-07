package edu.harvard.data.io;

import java.text.ParseException;
import java.util.Map;

import edu.harvard.data.DataTable;
import edu.harvard.data.VerificationException;

public interface JsonDocumentParser {
  Map<String, ? extends DataTable> getDocuments(Map<String, Object> values) throws ParseException, VerificationException;
}
