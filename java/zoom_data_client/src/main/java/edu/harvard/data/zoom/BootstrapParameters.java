package edu.harvard.data.zoom;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class BootstrapParameters {
	  private String configPathString;
	  private boolean downloadOnly;
	  private boolean createPipeline=true;
	  private Map<String,String> rapidConfigDict;
	  private String message;

	  public String getConfigPathString() {
	    return configPathString;
	  }

	  public String getConfigPathStringKeyValue() {
		final String key = "\"configPathString\":\"";
		final String value = getConfigPathString();
		final String enclose = "\"";
	    return key.concat(value).concat(enclose);
	  }

	  public void setConfigPathString(final String configPathString) {
	    this.configPathString = configPathString;
	  }

	  public boolean getDownloadOnly() {
	    return downloadOnly;
	  }

	  public boolean getCreatePipeline() {
		return createPipeline;
	  }

	  public void setDownloadOnly(final boolean downloadOnly) {
	    this.downloadOnly = downloadOnly;
	  }
	  
	  public void setCreatePipeline(final boolean createPipeline) {
		this.createPipeline = createPipeline;
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
	  
	  public boolean isRapidConfigDictEmpty() {
		  return rapidConfigDict.isEmpty();
	  }

	  public String getMessage() {
	    return message;
	  }

	  public void setMessage(final String message) {
	    this.message = message;
	  }


	  @Override
	  public String toString() {
	    return "BootstrapParams\n  ConfigPath: " + getConfigPathString()
		+ "\n  createPipeline: " + getCreatePipeline()
		+ "\n  downloadOnly: " + getDownloadOnly()
		+ "\n  rapidConfigDict: " + getRapidConfigDictString();
	  }

	  public String getRequestString() {
		  final String start = "{";
		  final String end= "}";
		  final String comma= ",";
		  final String total = getConfigPathStringKeyValue().concat(comma)
				  											.concat(getRapidConfigDictString());
		  final String all = start.concat(total).concat(end);
		  return all;
	  }

}
