package edu.harvard.data.mediasites;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataTable;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.FormatLibrary.Format;
import edu.harvard.data.TableFormat;
import edu.harvard.data.TableFormat.Compression;
import edu.harvard.data.io.FileTableReader;
import edu.harvard.data.io.JsonFileReader;
import edu.harvard.data.io.TableWriter;
import edu.harvard.data.mediasites.MediasitesDataConfig;
// Start
import edu.harvard.data.mediasites.bindings.phase0.Phase0Presentations;
import edu.harvard.data.mediasites.bindings.phase0.Phase0ViewingTrends;
import edu.harvard.data.mediasites.bindings.phase0.Phase0ViewingTrendsUsers;
import edu.harvard.data.mediasites.bindings.phase0.Phase0ViewingSessions;

// End
import edu.harvard.data.pipeline.InputTableIndex;

public class InputParser {

  private static final Logger log = LogManager.getLogger();

  private final MediasitesDataConfig config;
  private final AwsUtils aws;
  private final S3ObjectId inputObj;
  
  private File dataproductFile;
  private S3ObjectId dataproductOutputObj;
  
  private final String key;
  private final String filename;
  
  private final String currentDataProduct;
  private final String dataproductPrefix;

  private File originalFile;

  private final TableFormat inFormat;
  private final TableFormat outFormat;
  //Start 
  private final S3ObjectId presentationsOutputDir;
  private final S3ObjectId vtrendsOutputDir;
  private final S3ObjectId vtrendsusersOutputDir;
  private final S3ObjectId vsessionsOutputDir;
  //End

  public InputParser(final MediasitesDataConfig config, final AwsUtils aws,
      final S3ObjectId inputObj, final S3ObjectId outputLocation) {
    this.config = config;
    this.aws = aws;
    this.inputObj = inputObj;
    this.key = inputObj.getKey();
	this.filename = key.substring(key.lastIndexOf("/") + 1);	  
    this.dataproductPrefix = "PrepMediasites-";
    this.currentDataProduct = getDataProduct();
    this.presentationsOutputDir = AwsUtils.key( outputLocation, "Presentations");
    this.vtrendsOutputDir = AwsUtils.key( outputLocation, "ViewingTrends" );
    this.vtrendsusersOutputDir = AwsUtils.key( outputLocation, "ViewingTrendsUsers" );
    this.vsessionsOutputDir = AwsUtils.key( outputLocation, "ViewingSessions");
    final FormatLibrary formatLibrary = new FormatLibrary();
    this.inFormat = formatLibrary.getFormat(Format.Mediasites);
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
    final String dataproduct = filename.substring( filename.lastIndexOf(dataproductPrefix)+dataproductPrefix.length(), filename.lastIndexOf("-"));
    return dataproduct;
  }
  
  private void getFileName() {
    originalFile = new File(config.getScratchDir(), filename);

    final String dataproductFilename = currentDataProduct + ".gz";
    dataproductFile = new File(config.getScratchDir(), dataproductFilename );
    
    if (currentDataProduct.equals("Presentations") ) {
        dataproductOutputObj = AwsUtils.key(presentationsOutputDir, dataproductFilename );   	
    } else if ( currentDataProduct.equals("ViewingTrends") ) {
        dataproductOutputObj = AwsUtils.key(vtrendsOutputDir, dataproductFilename);    	    	
    } else if ( currentDataProduct.equals("ViewingTrendsUsers") ) {
        dataproductOutputObj = AwsUtils.key(vtrendsusersOutputDir, dataproductFilename);
    } else if ( currentDataProduct.equals("ViewingSessions") ) {
      dataproductOutputObj = AwsUtils.key(vsessionsOutputDir, dataproductFilename);    	
    }
    log.info("Parsing " + filename + " to " + dataproductFile);
    log.info("DataProduct Key: " + dataproductOutputObj );
    
  }

  private void parse() throws IOException {
    log.info("Parsing file " + originalFile);
    if (currentDataProduct.equals("Presentations")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0Presentations> presentations = new TableWriter<Phase0Presentations>(Phase0Presentations.class, outFormat,
    	            dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    	              presentations.add((Phase0Presentations) tables.get("Presentations").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("ViewingTrends")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0ViewingTrends> vtrends = new TableWriter<Phase0ViewingTrends>(Phase0ViewingTrends.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    				vtrends.add((Phase0ViewingTrends) tables.get("ViewingTrends").get(0));
    		}
    	}			
	} else if (currentDataProduct.equals("ViewingTrendsUsers")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    			TableWriter<Phase0ViewingTrendsUsers> vtrendsusers = new TableWriter<Phase0ViewingTrendsUsers>(Phase0ViewingTrendsUsers.class, outFormat,
    		            dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    				vtrendsusers.add((Phase0ViewingTrendsUsers) tables.get("ViewingTrendsUsers").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("ViewingSessions")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    			TableWriter<Phase0ViewingSessions> vsessions = new TableWriter<Phase0ViewingSessions>(Phase0ViewingSessions.class, outFormat,
    		            dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    				vsessions.add((Phase0ViewingSessions) tables.get("ViewingSessions").get(0));
    		}
    	}
	}
    log.info("Done Parsing file " + originalFile);
  }

  @SuppressWarnings("unused") // We run through each table's iterator, but don't
  // need the values.
  private void verify() throws IOException {
    if (currentDataProduct.equals("Presentations")) {

	    try(FileTableReader<Phase0Presentations> in = new FileTableReader<Phase0Presentations>(Phase0Presentations.class,
		    outFormat, dataproductFile)) {
	      log.info("Verifying file " + dataproductFile);	
	      for (final Phase0Presentations i : in ) {
	      }
	    }
    } else if (currentDataProduct.equals("ViewingTrends")) {
	    try(FileTableReader<Phase0ViewingTrends> in = new FileTableReader<Phase0ViewingTrends>(Phase0ViewingTrends.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0ViewingTrends i : in ) {
	      }
	    }
    } else if (currentDataProduct.equals("ViewingTrendsUsers")) {
	
	    try(FileTableReader<Phase0ViewingTrendsUsers> in = new FileTableReader<Phase0ViewingTrendsUsers>(Phase0ViewingTrendsUsers.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0ViewingTrendsUsers i : in ) {
	      }
        }
    } else if (currentDataProduct.equals("ViewingSessions")) {
    	
	    try(FileTableReader<Phase0ViewingSessions> in = new FileTableReader<Phase0ViewingSessions>(Phase0ViewingSessions.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);
	      for (final Phase0ViewingSessions i : in ) {
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
