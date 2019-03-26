package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.DataConfig;

public class RapidTransformGenerator {

	  private final S3ObjectId workingDir;
	  private final DataConfig config;
	  private final File dir;
	  private final GenerationSpec spec;

	  public RapidTransformGenerator(final File dir, final GenerationSpec spec, final DataConfig config,
		      final S3ObjectId workingDir) {
		    this.config = config;
		    this.dir = dir;
		    this.workingDir = workingDir;
		    this.spec = spec;
	  }
	  
	  public void generate() throws IOException {
		    final File rapidRequestFile = new File( dir, config.getRapidConfigFile() );

		    try (final PrintStream out = new PrintStream(new FileOutputStream(rapidRequestFile))) {
		      generateRapidRequests(out);
		    }
	  }
	  
	  private void generateRapidRequests(final PrintStream out) {
		  /*
		  import os
		  from collections import OrderedDict
		  from research_data_tools.dataRequestManager import DataRequestManager as DRM
		  tdr_path =os.path.join(os.path.dirname(__file__), '../../rapid-requests')

		  def make_request(filename):
		      return DRM.loadRequest(os.path.join(tdr_path, filename))

		  RUNTIME_DICT = OrderedDict()

		  RUNTIME_DICT['Recs'] = make_request('Apps/Apps-Faculty-Links/Person-Links-Recommendations.json')
		  */
		  final String rapidConfig = spec.getBootstrapRapidConfig();
		  final String rapidRequestDir = config.getRapidRequestsDir();
		  final String rapidCodeDir = config.getRapidCodeDir();
		  
		  /*
		   * Things to parse
		   *  - tdr path
		   *  - Runtime Dict
		   */
		  
		  // Setup
		  out.println("import os");
		  out.println("from collections import OrderedDict");
		  out.println("from research_data_tools.dataRequestManager import DataRequestManager as DRM");
		  out.println("tdr_path = os.path.join(os.path.dirname(__file__), \"" + rapidRequestDir + "\")");

		  // Make Request method
		  out.println("def make_request(filename):");
		  out.println("    return DRM.loadRequest(os.path.join(tdr_path, filename))");
		  out.println("");

		  // Requests dictionary
		  out.println("RUNTIME_DICT = OrderedDict()");
		  out.println("RUNTIME_DICT['Recs'] = make_request('Apps/Apps-Faculty-Links/Person-Links-Recommendations.json')");
	  }
	  
}

