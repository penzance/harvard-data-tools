package edu.harvard.data.linksapp;

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
import edu.harvard.data.linksapp.LinksappDataConfig;
import edu.harvard.data.pipeline.InputTableIndex;

public class LinksappPhase0 extends Phase0 {

  private static final Logger log = LogManager.getLogger();
  private final LinksappDataConfig config;
  private final String runId;
  private final ExecutorService exec;

  public LinksappPhase0(final LinksappDataConfig config, final String runId,
      final ExecutorService exec) {
    this.config = config;
    this.runId = runId;
    this.exec = exec;
  }

  @Override
  protected ReturnStatus run() throws IOException, InterruptedException, ExecutionException {
    log.info("Starting Links Phase0...");
    // Start
    final AwsUtils aws = new AwsUtils();
    final InputTableIndex dataIndex = new InputTableIndex();
    final List<Future<InputTableIndex>> jobs = new ArrayList<Future<InputTableIndex>>();
    for (final S3ObjectSummary obj : aws.listKeys(config.getDropboxBucket())) {
        if (obj.getKey().endsWith(".gz")) {
            final S3ObjectId outputLocation = AwsUtils.key(config.getS3WorkingLocation(runId));
            final LinksappSingleFileParser parser = new LinksappSingleFileParser(obj,
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
       if ( table.equals("ScraperText") ) {
            dataIndex.setPartial(table, true); 	
       } else if ( table.equals("ScraperCitations") ) {
           dataIndex.setPartial(table, true); 
       } else if ( table.equals("ScraperCitationsCatalyst") ) {
           dataIndex.setPartial(table, true); 
       } else if ( table.equals("OpenScholarPages") ) {
           dataIndex.setPartial(table, true); 
       } else if ( table.equals("OpenScholarBiblioTitles") ) {
           dataIndex.setPartial(table, true); 
       } else if ( table.equals("OpenScholarMapping") ) {
           dataIndex.setPartial(table, true); 
       } else if ( table.equals("Gazette") ) {
           dataIndex.setPartial(table, true); 
       } else if ( table.equals("GazetteEvents") ) {
           dataIndex.setPartial(table, true); 
       } else if ( table.equals("Rss") ) {
           dataIndex.setPartial(table, true); 
       } else if ( table.equals("HiltIntoPractice") ) {
           dataIndex.setPartial(table, true); 
       } else if ( table.equals("ScraperTextEntity") ) {
           dataIndex.setPartial(table, true); 
       }
    }
    aws.writeJson(config.getIndexFileS3Location(runId), dataIndex );
    
    return ReturnStatus.OK;
    // End
  }
}

class LinksappSingleFileParser implements Callable<InputTableIndex> {
	  private static final Logger log = LogManager.getLogger();

	  private final S3ObjectSummary inputObj;
	  private final LinksappDataConfig config;
	  private final S3ObjectId outputLocation;
	  private final AwsUtils aws;

	  public LinksappSingleFileParser(final S3ObjectSummary inputObj, final S3ObjectId outputLocation,
	      final LinksappDataConfig config) {
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

