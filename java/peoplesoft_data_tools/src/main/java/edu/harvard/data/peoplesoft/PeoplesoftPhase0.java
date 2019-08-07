package edu.harvard.data.peoplesoft;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.Phase0;
import edu.harvard.data.ReturnStatus;
import edu.harvard.data.peoplesoft.PeoplesoftDataConfig;
import edu.harvard.data.pipeline.InputTableIndex;

public class PeoplesoftPhase0 extends Phase0 {

  private static final Logger log = LogManager.getLogger();
  private final PeoplesoftDataConfig config;
  private final String runId;
  private final ExecutorService exec;

  public PeoplesoftPhase0(final PeoplesoftDataConfig config, final String runId,
      final ExecutorService exec) {
    this.config = config;
    this.runId = runId;
    this.exec = exec;
  }

  @Override
  protected ReturnStatus run() throws IOException, InterruptedException, ExecutionException {
    log.info("Starting Peoplesoft Phase0...");
    // Start
    final AwsUtils aws = new AwsUtils();
    final InputTableIndex dataIndex = new InputTableIndex();
    final List<Future<InputTableIndex>> jobs = new ArrayList<Future<InputTableIndex>>();
    for (final S3ObjectSummary obj : aws.listKeys(config.getDropboxBucket())) {
        if (obj.getKey().endsWith(".gz")) {
            final S3ObjectId outputLocation = AwsUtils.key(config.getS3WorkingLocation(runId));
            final PeoplesoftSingleFileParser parser = new PeoplesoftSingleFileParser(obj,
            	outputLocation, config);
            jobs.add(exec.submit(parser));
            log.info("Queuing file " + obj.getBucketName() + "/" + obj.getKey());
        }
    }
    for (final Future<InputTableIndex> job : jobs ) {
       dataIndex.addAll(job.get());
    }
    dataIndex.setSchemaVersion("1.0");
    for (final String table : dataIndex.getTableNames()) {
       if ( table.equals("Appointments") ) {
            dataIndex.setPartial(table, true); 	
       } else if ( table.equals("CurrentActive")) {
            dataIndex.setPartial(table, false); 	
       } else if ( table.equals("HistoricActive")) {
            dataIndex.setPartial(table, false); 	
       }
    }
    aws.writeJson(config.getIndexFileS3Location(runId), dataIndex );
    
    return ReturnStatus.OK;
    // End
  }
}

class PeoplesoftSingleFileParser implements Callable<InputTableIndex> {
	  private static final Logger log = LogManager.getLogger();

	  private final S3ObjectSummary inputObj;
	  private final PeoplesoftDataConfig config;
	  private final S3ObjectId outputLocation;
	  private final AwsUtils aws;

	  public PeoplesoftSingleFileParser(final S3ObjectSummary inputObj, final S3ObjectId outputLocation,
	      final PeoplesoftDataConfig config) {
	    this.inputObj = inputObj;
	    this.outputLocation = outputLocation;
	    this.config = config;
	    this.aws = new AwsUtils();
	  }

	  @Override
	  public InputTableIndex call() throws Exception {
	    log.info("Parsing file " + inputObj.getBucketName() + "/" + inputObj.getKey());
	    final InputParser parser = new InputParser(config, aws, AwsUtils.key(inputObj), outputLocation);
	    return parser.parseFile();
	  }

	}

