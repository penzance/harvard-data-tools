package edu.harvard.data.canvas;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class BootstrapParameters {
  private String configPathString;
  private Integer dumpSequence;
  private String table;
  private boolean createPipeline;
  private Map<String,String> rapidConfigDict;
  private boolean downloadOnly;

  public String getConfigPathString() {
    return configPathString;
  }

  public void setConfigPathString(final String configPathString) {
    this.configPathString = configPathString;
  }

  public Integer getDumpSequence() {
    return dumpSequence;
  }

  public void setDumpSequence(final Integer dumpSequence) {
    this.dumpSequence = dumpSequence;
  }

  public String getTable() {
    return table;
  }

  public void setTable(final String table) {
    this.table = table;
  }

  public boolean getDownloadOnly() {
    return downloadOnly;
  }
  
  public boolean getCreatePipeline() {
    try {
	  if (this.getRapidConfigDict().containsKey("createPipeline")) {
		 return Boolean.parseBoolean(this.getRapidConfigDict().get("createPipeline"));
	  } else return true; // Set Default Pipeline setting here
    } catch (NullPointerException e) {
      return true;
    }
  }

  public void setDownloadOnly(final boolean downloadOnly) {
    this.downloadOnly = downloadOnly;
  }

  public void setCreatePipeline() {
	this.createPipeline = getCreatePipeline();
  }
  
  public void setRapidDictString( final Map<String,String> rapidConfigDict) {
	 this.rapidConfigDict = rapidConfigDict;
  }
  
  public String getRapidConfigDictString() {
	  
	try {
	  final String key = "\"rapidConfigDict\":";
	  final String value = mapToString(getRapidConfigDict());
      return key.concat(value);
    } catch (NullPointerException e) {
      return "null";
    }
  }
  
  public String mapToString(final Map<String, String> mapDict) {
        StringBuilder sb = new StringBuilder();
        Iterator<Entry<String, String>> iter = mapDict.entrySet().iterator();
        sb.append("{");
        while (iter.hasNext()) {
            Entry<String, String> entry = iter.next();
            sb.append('"');
            sb.append(entry.getKey());
            sb.append('"');
            sb.append(':').append('"');
            sb.append(entry.getValue());
            sb.append('"');
            if (iter.hasNext()) {
                sb.append(',').append(' ');
            }
        }
        sb.append("}");
        return sb.toString();
  }
  
  public Map<String,String> getRapidConfigDict() {
	return rapidConfigDict;
  }

  @Override
  public String toString() {
    return "BootstrapParams\n  ConfigPath: " + getConfigPathString()
		+ "\n  dumpSequence: " + getDumpSequence()
		+ "\n  createPipeline: " + getCreatePipeline()
		+ "\n  downloadOnly: " + getDownloadOnly()
		+ "\n  rapidConfigDict: " + getRapidConfigDictString();
  }

}
