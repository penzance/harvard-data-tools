package edu.harvard.data.links;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class BootstrapParameters {
	  private String configPathString;
	  private boolean downloadOnly;
	  private boolean createPipeline;
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
		if (this.getRapidConfigDict().containsKey("createPipeline")) {
			return Boolean.parseBoolean(this.getRapidConfigDict().get("createPipeline"));
		} else return true; // Set Default Pipeline setting here
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
		  
		final String key = "\"rapidConfigDict\":";
		final String value = mapToString(getRapidConfigDict());
	    return key.concat(value);
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

	  public String getMessage() {
	    return message;
	  }

	  public void setMessage(final String message) {
	    this.message = message;
	  }

	  @Override
	  public String toString() {
	    return "BootstrapParams\n  ConfigPath: " + configPathString
	    	+ "\n  message: " + message
			+ "\n  createPipeline: " + createPipeline
			+ "\n  downloadOnly: " + downloadOnly 
			+ "\n  rapidConfigDict: " + rapidConfigDict.toString();
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
