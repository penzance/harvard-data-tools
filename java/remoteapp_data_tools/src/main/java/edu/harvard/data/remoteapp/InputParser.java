package edu.harvard.data.remoteapp;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataTable;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.TableFormat.Compression;
import edu.harvard.data.io.FileTableReader;
import edu.harvard.data.io.JsonFileReader;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.remoteapp.EventJsonDocumentParser;
import edu.harvard.data.remoteapp.bindings.phase0.Phase0Meetings;
import edu.harvard.data.remoteapp.bindings.phase0.Phase0Activities;
import edu.harvard.data.remoteapp.bindings.phase0.Phase0Participants;
import edu.harvard.data.remoteapp.bindings.phase0.Phase0Quality;
import edu.harvard.data.pipeline.InputTableIndex;
import edu.harvard.data.remoteapp.RemoteappDataConfig;

public class InputParser {

  private static final Logger log = LogManager.getLogger();

  private final RemoteappDataConfig config;
  private final AwsUtils aws;
  private final S3ObjectId inputObj;
  
  private File dataproductFile;
  private S3ObjectId dataproductOutputObj;
  
  private final String key;
  private final String filename;
  
  private final String currentDataProduct;
  private final String dataproductPrefix;
  private final String dataproductFiletype;

  private File originalFile;

  private final TableFormat inFormat;
  private final TableFormat outFormat; 
  private final S3ObjectId remoteappMeetingsOutputDir;
  private final S3ObjectId remoteappActivitiesOutputDir;
  private final S3ObjectId remoteappParticipantsOutputDir;
  private final S3ObjectId remoteappQualitiesOutputDir;


  public InputParser(final RemoteappDataConfig config, final AwsUtils aws,
      final S3ObjectId inputObj, final S3ObjectId outputLocation) {
    this.config = config;
    this.aws = aws;
    this.inputObj = inputObj;
    this.key = inputObj.getKey();
	this.filename = key.substring(key.lastIndexOf("/") + 1);	  
    this.dataproductPrefix = "PrepRemoteapp_";
    this.dataproductFiletype = ".json.gz";
    this.currentDataProduct = getDataProduct();
    this.remoteappMeetingsOutputDir = AwsUtils.key(outputLocation, "Meetings");
    this.remoteappActivitiesOutputDir= AwsUtils.key(outputLocation, "Activities");
    this.remoteappParticipantsOutputDir= AwsUtils.key(outputLocation, "Participants");
    this.remoteappQualitiesOutputDir= AwsUtils.key(outputLocation, "Quality");
    final FormatLibrary formatLibrary = new FormatLibrary();
    this.inFormat = formatLibrary.getFormat(Format.Sis);
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setSerializationInclusion(Include.NON_NULL);    
    this.inFormat.setJsonMapper(jsonMapper);
    this.outFormat = formatLibrary.getFormat(config.getPipelineFormat());
    this.outFormat.setCompression(Compression.Gzip);
  }

  public InputTableIndex parseFile() throws IOException {
    final InputTableIndex dataIndex = new InputTableIndex();
    try {
      getFileName();
      aws.getFile(inputObj, originalFile);
      parse();
      verify();
      // Add product check here
      aws.putFile(dataproductOutputObj, dataproductFile);
      // Add product check here
      dataIndex.addFile(currentDataProduct, dataproductOutputObj, dataproductFile.length());
    } finally {
      cleanup();
    }
    return dataIndex;
  }
  
  private final String getDataProduct() {
    final String dataproduct = filename.substring( filename.lastIndexOf(dataproductPrefix)+dataproductPrefix.length() ).replace(dataproductFiletype, "");
    return dataproduct;
  }
  
  private void getFileName() {
    originalFile = new File(config.getScratchDir(), filename);

    final String dataproductFilename = currentDataProduct + ".gz";
    dataproductFile = new File(config.getScratchDir(), dataproductFilename );
    
    if (currentDataProduct.equals("Meetings") ) {
        dataproductOutputObj = AwsUtils.key(remoteappMeetingsOutputDir, dataproductFilename );  
    }
    else if (currentDataProduct.equals("Activities") ) {
        dataproductOutputObj = AwsUtils.key(remoteappActivitiesOutputDir, dataproductFilename );  
    }
    else if (currentDataProduct.equals("Participants") ) {
        dataproductOutputObj = AwsUtils.key(remoteappParticipantsOutputDir, dataproductFilename );  
    }
    else if (currentDataProduct.equals("Quality") ) {
        dataproductOutputObj = AwsUtils.key(remoteappQualitiesOutputDir, dataproductFilename );  
    }
    
    log.info("Parsing " + filename + " to " + dataproductFile);
    log.info("DataProduct Key: " + dataproductOutputObj );
    
  }

  private void parse() throws IOException {
    log.info("Parsing file " + originalFile);
    if (currentDataProduct.equals("Meetings")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0Meetings> meetings = new TableWriter<Phase0Meetings>(Phase0Meetings.class, outFormat,
    	            dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  meetings.add((Phase0Meetings) tables.get("Meetings").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("Activities")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0Activities> activities = new TableWriter<Phase0Activities>(Phase0Activities.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  activities.add((Phase0Activities) tables.get("Activities").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("Participants")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0Participants> participants = new TableWriter<Phase0Participants>(Phase0Participants.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  participants.add((Phase0Participants) tables.get("Participants").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("Quality")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0Quality> participants = new TableWriter<Phase0Quality>(Phase0Quality.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  participants.add((Phase0Quality) tables.get("Quality").get(0));
    		}
    	}
	}
    log.info("Done Parsing file " + originalFile);
  }

  @SuppressWarnings("unused") // We run through each table's iterator, but don't
  // need the values.
  private void verify() throws IOException {
    if (currentDataProduct.equals("Meetings")) {

	    try(FileTableReader<Phase0Meetings> in = new FileTableReader<Phase0Meetings>(Phase0Meetings.class,
		    outFormat, dataproductFile)) {
	      log.info("Verifying file " + dataproductFile);	
	      for (final Phase0Meetings i : in ) {
	      }
	    }
    } else if (currentDataProduct.equals("Activities")) {

	    try(FileTableReader<Phase0Activities> in = new FileTableReader<Phase0Activities>(Phase0Activities.class,
		    outFormat, dataproductFile)) {
	      log.info("Verifying file " + dataproductFile);	
	      for (final Phase0Activities i : in ) {
	      }
	    }
    } else if (currentDataProduct.equals("Participants")) {

	    try(FileTableReader<Phase0Participants> in = new FileTableReader<Phase0Participants>(Phase0Participants.class,
		    outFormat, dataproductFile)) {
	      log.info("Verifying file " + dataproductFile);	
	      for (final Phase0Participants i : in ) {
	      }
	    }
    } else if (currentDataProduct.equals("Quality")) {

	    try(FileTableReader<Phase0Quality> in = new FileTableReader<Phase0Quality>(Phase0Quality.class,
		    outFormat, dataproductFile)) {
	      log.info("Verifying file " + dataproductFile);	
	      for (final Phase0Quality i : in ) {
	      }
	    }
    }
    
  }

  private void cleanup() {
    if (originalFile != null && originalFile.exists()) {
      originalFile.delete();
    }
    // STart
    if (dataproductFile != null && dataproductFile.exists()) {
    	dataproductFile.delete();
    }
  }

}