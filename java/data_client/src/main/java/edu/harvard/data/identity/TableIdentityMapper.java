package edu.harvard.data.identity;

import java.util.Map;

import org.apache.commons.csv.CSVRecord;

public interface TableIdentityMapper<T> {

  void readRecord(CSVRecord csvRecord);

  // Get a map in case there are two identifier fields but one of them is null.
  // This way we register that we're dealing with the second case in the map
  // method. If it was a set then the null identifier wouldn't show up, so it
  // would appear that there's only one identifier.
  Map<String, T> getHadoopKeys();

  boolean populateIdentityMap(IdentityMap id);

}
