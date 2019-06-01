package edu.harvard.data.links;
import java.util.Map;

public class BootstrapParameters {
	  private String configPathString;
	  private boolean downloadOnly;
	  private Map<String,String> rapidConfigDict;
	  private String message;

	  public String getConfigPathString() {
	    return configPathString;
	  }

	  public void setConfigPathString(final String configPathString) {
	    this.configPathString = configPathString;
	  }

	  public boolean getDownloadOnly() {
	    return downloadOnly;
	  }

	  public void setDownloadOnly(final boolean downloadOnly) {
	    this.downloadOnly = downloadOnly;
	  }
	  
	  public void setRapidDictString( final Map<String,String> rapidConfigDict) {
		 this.rapidConfigDict = rapidConfigDict;
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
	    return "BootstrapParams\n  ConfigPath: " + configPathString + "\n  message: "
			+ message + "\n  downloadOnly: " + downloadOnly + "\n  rapidConfigDict: "
			+ rapidConfigDict.toString();
	  }
}