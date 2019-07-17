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
		  /*
		   * 1/ Generate Custom datarequest_config.py
		   * 2/ Generate Custom Bash script for Rapid
		   */
		  
		  // Generate Custom datarequest_config.py
		  final File rapidRequestFile = new File( dir, config.getRapidConfigFile() );
		    try (final PrintStream out = new PrintStream(new FileOutputStream(rapidRequestFile))) {
		      generateRapidRequests(out);
		    }
		    
		  // Generate Custom Bash Script for Rapid
		  final File rapidScriptFile = new File( dir, config.getRapidScriptFile() );
		    try (final PrintStream out = new PrintStream(new FileOutputStream(rapidScriptFile))) {
		      generateRapidScript(out);
		    }
	  }
	  
	  private void generateRapidScript(final PrintStream out) {
		  /*
		   * Generate phase_3_rapid.sh bash script to run RAPID Transformations
		   */
		  
		  out.println("#!/bin/bash");
		  out.println();
	      
	      // Clean Env Vars
		  out.println();
          out.println("unset AWS_ACCESS_KEY_ID\n"
    	 	        + "unset AWS_SECRET_ACCESS_KEY\n"
    		        + "unset AWS_SESSION_TOKEN\n");

		  // DEBUG
		  out.println("");
		  out.println("# Debug env vars");
		  out.println("env >> /var/log/rapid-transform.out");
          
          // Export Vars
		  out.println();
          out.println("export RAPID_CODE_BASE=" + config.getRapidCodeDir() + "\n"
    	 	        + "export RAPID_REQUESTS_BASE=" + config.getRapidRequestsDir() + "\n"
    		        + "sudo yum install jq\n");

          // CONFIG
		  out.println();
          out.println("aws s3 cp " + config.getRapidGoogleCreds() + " " + config.getRapidGoogleDir() + "\n"
    	 	        + "mkdir /tmp/OUTPUT\n"
        		    + "mkdir /tmp/LOGS\n");

          // GITHUB API TOKEN
		  out.println();
          out.println("aws configure set default.region '" + config.getRapidAwsDefaultRegion() + "'\n"
    	 	        + "GITHUB_TOKEN=`aws ssm get-parameter --name " + config.getRapidGithubTokenName()  
    	 	        + " | jq -r '.Parameter.Value'`\n");
          
          // Pull latest Rapid Code + Requests
		  out.println();
          out.println("login $GITHUB_TOKEN\n"
    	 	        + "git clone -b " + config.getRapidGithubBranch() + " "
    	 	        + config.getRapidGithubUrl() + " $RAPID_CODE_BASE\n"
    		        + "git clone -b " + config.getRapidGithubRequestBranch() + " "
    		        + config.getRapidGithubRequestUrl() + " $RAPID_REQUESTS_BASE\n");
          
          // RAPID CONFIG Test
		  out.println();
		  out.println("cat << EOF > $RAPID_CODE_BASE/config_init.py\n"
					+ "#!/usr/bin/python\n"
					+ "rs_config_key='" + config.getRapidRsConfigKey() + "'\n"
					+ "canvas_config_region='" + config.getRapidCanvasConfigRegion() + "'\n"
					+ "canvas_config_key='" + config.getRapidCanvasConfigKey() + "'\n"
					+ "rs_config_region='" + config.getRapidRsConfigRegion() + "'\n"
					+ "canvas_config_bucket='" + config.getRapidCanvasConfigBucket() + "'\n"
					+ "rs_config_bucket='" + config.getRapidRsConfigBucket() + "'\n"
					+ "EOF\n");
		  out.println("chmod 755 $RAPID_CODE_BASE/config_init.py\n");
			
		  // Set Default AWS creds 
		  out.println();
		  out.println("# Set RAPID Code Env vars\n"
		            + "aws configure set default.region '" + config.getRapidAwsDefaultRegion() + "'\n"
		            + "AWS_ID=`aws iam list-access-keys --user-name " + config.getRapidAwsDefaultAccessKeyUsername() + " | jq -r '.AccessKeyMetadata[0].AccessKeyId'`\n"
		            + "AWS_KEY=`aws ssm get-parameter --name " + config.getRapidAwsDefaultAccessSecretKey() + " | jq -r '.Parameter.Value'`\n");
		            
		  
		  // Write Creds
		  out.println();
		  out.println("# Set to VPAL Apps Creds on main AWS Account\n"
			        + "echo \"[default]\" >> ~/.aws/credentials\n"
			        + "echo \"aws_access_key_id=$AWS_ID\" >> ~/.aws/credentials\n"
			        + "echo \"aws_secret_access_key=$AWS_KEY\" >> ~/.aws/credentials\n");

		  
		  // Write Cross account info to Environment
		  out.println();
		  out.println("# NEW: Get across account data\n"
				    + "AWS_TEMP=`aws sts assume-role --role-arn \"" + config.getRapidAwsAssumeRoleArn()
				    + "\" --role-session-name \"" + config.getRapidAwsAssumeRoleSessionName() + "\""
				    + " --duration-seconds " + config.getRapidAwsAssumeRoleDuration() + "`\n"
				    + "AWS_ACCESS_KEY_ID=`echo $AWS_TEMP | jq -r '.Credentials.AccessKeyId'`\n"
				    + "AWS_SECRET_ACCESS_KEY=`echo $AWS_TEMP | jq -r '.Credentials.SecretAccessKey'`\n"
				    + "AWS_SESSION_TOKEN=`echo $AWS_TEMP | jq -r '.Credentials.SessionToken'`\n"
				    + "export AWS_ACCESS_KEY_ID=\"$AWS_ACCESS_KEY_ID\"\n"
				    + "export AWS_SECRET_ACCESS_KEY=\"$AWS_SECRET_ACCESS_KEY\"\n"
				    + "export AWS_SESSION_TOKEN=\"$AWS_SESSION_TOKEN\"\n"
				    + "env\n");
		  
		  // Copy runtime generated data request
		  // TODO: Disable for now until dynamically generated Lambda json , and use dataconfig_g
		  //out.println();
		  //out.println("# Update datarequest config with Runtime generated code");
		  //out.println("cp " + config.getEmrCodeDir() + "/"
			//	  	+ config.getRapidConfigFile() + " " + config.getRapidCodeDir() + "/runtime/datarequest_config.py");
		  
		  // EMR specific 
		  out.println();
		  out.println("# Remove Maven Repo dependencies (by renaming) to avoid pyspark config download conflicts");
		  out.println("mv /home/hadoop/.m2/ /home/hadoop/.rmm2");
		  out.println("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native:/usr/lib/hadoop-lzo/lib/native");

		  // DEBUG
		  out.println("");
		  out.println("#Debug env vars");
		  out.println("echo $SPARK_CLASSPATH >> /var/log/rapid-transform.out");
		  out.println("echo $SPARK_LIBRARY_PATH >> /var/log/rapid-transform.out");
		  

		  
		  // Run Rapid Code
		  out.println();
		  out.println("#Run RAPID Code\n"
				    + "cd $RAPID_CODE_BASE\n"
				    + "python runtime/main.py --metadata --runtime >> /var/log/rapid-transform.out\n"
				    + "python runtime/main.py --data-requests --runtime >> /var/log/rapid-transform.out\n"
				    + "python runtime/main.py --alert --runtime >> /var/log/rapid-transform.out\n");
		  out.println();
		  
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
		  out.println("RUNTIME_DICT['PERSON-LINKS-EVENTS'] = make_request('Apps/Apps-Faculty-Links/Person-Links-Events-Dev-EMR.json')");
	  }
	  
}

