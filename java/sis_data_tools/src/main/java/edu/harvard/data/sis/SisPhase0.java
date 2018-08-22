package edu.harvard.data.sis;

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
import edu.harvard.data.pipeline.InputTableIndex;

public class SisPhase0 extends Phase0 {

  private static final Logger log = LogManager.getLogger();
  private final SisDataConfig config;
  private final String runId;
  private final ExecutorService exec;

  public SisPhase0(final SisDataConfig config, final String runId,
      final ExecutorService exec) {
    this.config = config;
    this.runId = runId;
    this.exec = exec;
  }

  @Override
  protected ReturnStatus run() throws IOException, InterruptedException, ExecutionException {
    log.info("Starting Sis Phase0...");
    // Start
    final AwsUtils aws = new AwsUtils();
    final InputTableIndex dataIndex = new InputTableIndex();
    final List<Future<InputTableIndex>> jobs = new ArrayList<Future<InputTableIndex>>();
    for (final S3ObjectSummary obj : aws.listKeys(config.getDropboxBucket())) {
        if (obj.getKey().endsWith(".gz")) {
            final S3ObjectId outputLocation = AwsUtils.key(config.getS3WorkingLocation(runId));
            final SisSingleFileParser parser = new SisSingleFileParser(obj,
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
       if ( table.equals("CourseCatalog") ) {
            dataIndex.setPartial(table, true); 	
       } else if ( table.equals("Classes") ) {
           dataIndex.setPartial(table, true); 
       } else if ( table.equals("CourseMap") ) {
           dataIndex.setPartial(table, true);
       } else if ( table.equals("PrimeCourseEnroll") ) {
           dataIndex.setPartial(table, true);             
       } else if ( table.equals("CourseEnroll") ) {
           dataIndex.setPartial(table, true); 
       } else if ( table.equals("HistoricalCourse") ) {
           dataIndex.setPartial(table, true); 	
       } else if ( table.equals("HistoricalEnroll") ) {
           dataIndex.setPartial(table, true); 	
       }
    }
    aws.writeJson(config.getIndexFileS3Location(runId), dataIndex );
    
    return ReturnStatus.OK;
    // End
  }
}

class SisSingleFileParser implements Callable<InputTableIndex> {
	  private static final Logger log = LogManager.getLogger();

	  private final S3ObjectSummary inputObj;
	  private final SisDataConfig config;
	  private final S3ObjectId outputLocation;
	  private final AwsUtils aws;

	  public SisSingleFileParser(final S3ObjectSummary inputObj, final S3ObjectId outputLocation,
	      final SisDataConfig config) {
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

