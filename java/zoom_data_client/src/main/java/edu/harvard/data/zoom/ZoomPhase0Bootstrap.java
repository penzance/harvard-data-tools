package edu.harvard.data.zoom;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.*;
//import com.amazonaws.services.lambda.runtime.Context;
//import com.amazonaws.services.lambda.runtime.RequestHandler;
//import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfigurationException;
import edu.harvard.data.pipeline.Phase0Bootstrap;
import edu.harvard.data.schema.UnexpectedApiResponseException;
import edu.harvard.data.zoom.BootstrapParameters;
import edu.harvard.data.zoom.ZoomPhase0Bootstrap;

public class ZoomPhase0Bootstrap extends Phase0Bootstrap
implements RequestStreamHandler, RequestHandler<BootstrapParameters, String> {
	
  private static final Logger log = LogManager.getLogger();
  private BootstrapParameters params;
	   
  // Main method for testing
  public static void main(final String[] args) 
		      throws JsonParseException, JsonMappingException, IOException {
	log.info("Args: " + args.toString());
	System.out.println(args.toString());
	final BootstrapParameters params = new ObjectMapper().readValue(args[0],
		        BootstrapParameters.class);
	System.out.println(new ZoomPhase0Bootstrap().handleRequest(params, null));
  }	
  
  @Override
  public String handleRequest(final BootstrapParameters params, final Context context) {
	try {
		  log.info(params.getCreatePipeline());
	      super.init(params.getConfigPathString(), ZoomDataConfig.class, params.getCreatePipeline());
	      super.run(context);
	} catch (IOException | DataConfigurationException | UnexpectedApiResponseException e) {
	      return "Error: " + e.getMessage();
	}
	    return "";
  }	

  @Override
  public void handleRequest(InputStream inputStream, OutputStream outputStream, final Context context) {
	try {
	  final String requestjson = IOUtils.toString(inputStream, "UTF-8");
	  log.info("Params: " + requestjson);
      this.params = new ObjectMapper().readValue(requestjson, BootstrapParameters.class);
      log.info(params.getConfigPathString());
      log.info(params.getRapidConfigDict());
      log.info(params.getCreatePipeline());
	  super.init(params.getConfigPathString(), 
	    		 ZoomDataConfig.class, params.getCreatePipeline(), requestjson);
	  super.run(context);
	} catch (IOException | DataConfigurationException | UnexpectedApiResponseException e) {
	      log.info("Error: " + e.getMessage());
	}
  }
  
  @Override
  protected List<S3ObjectId> getInfrastructureConfigPaths() {
    final List<S3ObjectId> paths = new ArrayList<S3ObjectId>();
    final S3ObjectId configPath = AwsUtils.key(config.getCodeBucket(), "infrastructure");
    paths.add(AwsUtils.key(configPath, "tiny_phase_0.properties"));
    paths.add(AwsUtils.key(configPath, "tiny_emr_rapid.properties"));
    return paths;
  }

  @Override
  protected Map<String, String> getCustomEc2Environment() {
    final Map<String, String> env = new HashMap<String, String>();
    env.put("DATA_SET_ID", runId);
    env.put("DATA_SCHEMA_VERSION", "1.0");
    return env;
  }

  @Override
  protected boolean newDataAvailable()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
	return !aws.listKeys(((ZoomDataConfig)config).getDropboxBucket()).isEmpty();
  }

  @Override
  protected void setup()
      throws IOException, DataConfigurationException, UnexpectedApiResponseException {
  }

}
