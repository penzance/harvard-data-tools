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
import edu.harvard.data.mediasites.bindings.phase0.Phase0ViewingTrendsUser;
// End
import edu.harvard.data.pipeline.InputTableIndex;

public class InputParser {

  private static final Logger log = LogManager.getLogger();

  private final MediasitesDataConfig config;
  private final AwsUtils aws;
  private final S3ObjectId inputObj;
  // Start
  private File presentationsFile;
  private File vtrendsFile;
  private File vtrendsusersFile;
  // End
  private File originalFile;
  // Start
  private S3ObjectId presentationsOutputObj;
  private S3ObjectId vtrendsOutputObj;
  private S3ObjectId vtrendsusersOutputObj;
  // End 
  private final TableFormat inFormat;
  private final TableFormat outFormat;
  //Start 
  private final S3ObjectId presentationsOutputDir;
  private final S3ObjectId vtrendsOutputDir;
  private final S3ObjectId vtrendsusersOutputDir;
  //End

  public InputParser(final MediasitesDataConfig config, final AwsUtils aws,
      final S3ObjectId inputObj, final S3ObjectId outputLocation) {
    this.config = config;
    this.aws = aws;
    this.inputObj = inputObj;
    this.presentationsOutputDir = AwsUtils.key( outputLocation, "presentations ");
    this.vtrendsOutputDir = AwsUtils.key( outputLocation, "viewing_trends" );
    this.vtrendsusersOutputDir = AwsUtils.key( outputLocation, "viewing_trends_users" );
    final FormatLibrary formatLibrary = new FormatLibrary();
    this.inFormat = formatLibrary.getFormat(Format.Mediasites);
    this.outFormat = formatLibrary.getFormat(config.getPipelineFormat());
    this.outFormat.setCompression(Compression.Gzip);
  }

  public InputTableIndex parseFile() throws IOException {
    final InputTableIndex dataIndex = new InputTableIndex();
    try {
      getFileNames();
      aws.getFile(inputObj, originalFile);
      parse();
      verify();
      // start
      aws.putFile(presentationsOutputObj, presentationsFile);
      aws.putFile(vtrendsOutputObj, vtrendsFile);
      aws.putFile(vtrendsusersOutputObj, vtrendsusersFile);
      // end
      //start
      dataIndex.addFile("presentations", presentationsOutputObj, presentationsFile.length() );
      dataIndex.addFile("viewing_trends", vtrendsOutputObj, vtrendsFile.length());
      dataIndex.addFile("viewing_trends_users", vtrendsusersOutputObj, vtrendsusersFile.length());
      //end      
    } finally {
      cleanup();
    }
    return dataIndex;
  }

  private void getFileNames() {
    final String key = inputObj.getKey();
    final String filename = key.substring(key.lastIndexOf("/") + 1);
    final String date = filename.substring(filename.indexOf(".") + 1, filename.indexOf(".json"));
    originalFile = new File(config.getScratchDir(), filename);
    //Start
    final String presentationsFileName = "PrepMediasites-Presentations-" + date + ".gz";
    final String vtrendsFileName = "PrepMediasites-ViewingTrends-" + date + ".gz";
    final String vtrendsusersFileName = "PrepMediasites-ViewingTrendsUsers-" + date + ".gz";
    //End
    //Start
    presentationsFile = new File(config.getScratchDir(), presentationsFileName );
    vtrendsFile = new File(config.getScratchDir(), vtrendsFileName );
    vtrendsusersFile = new File(config.getScratchDir(), vtrendsusersFileName );
    //End
    //Start
    presentationsOutputObj = AwsUtils.key(presentationsOutputDir, presentationsFileName );
    vtrendsOutputObj = AwsUtils.key(vtrendsOutputDir, vtrendsFileName );
    vtrendsusersOutputObj = AwsUtils.key(vtrendsusersOutputDir, vtrendsusersFileName );
    //End
    //Start
    log.info("Parsing " + filename + " to " + presentationsFile + ", " + vtrendsFile + ", " + vtrendsusersFile );
    log.info("Presentations key: " + presentationsOutputObj );
    log.info("Viewing Trends key: " + vtrendsOutputObj );
    log.info("Viewing Trends Users key: " + vtrendsusersOutputObj );
    //End
  }

  private void parse() throws IOException {
    log.info("Parsing file " + originalFile);
    try (
        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
            new EventJsonDocumentParser(inFormat, true));
    	// Start
    	TableWriter<Phase0Presentations> presentations = new TableWriter<Phase0Presentations>(Phase0Presentations.class, outFormat,
    		presentationsFile);
    	TableWriter<Phase0ViewingTrends> vtrends = new TableWriter<Phase0ViewingTrends>(Phase0ViewingTrends.class, outFormat,
    		vtrendsFile);
    	TableWriter<Phase0ViewingTrendsUser> vtrendsusers = new TableWriter<Phase0ViewingTrendsUser>(Phase0ViewingTrendsUser.class, outFormat,
    		vtrendsusersFile);       	    		
    	// End    		
    		) {
      for (final Map<String, List<? extends DataTable>> tables : in) {
    	  presentations.add((Phase0Presentations) tables.get("presentations").get(0));
    	  vtrends.add((Phase0ViewingTrends) tables.get("viewing_trends").get(0));
    	  vtrendsusers.add((Phase0ViewingTrendsUser) tables.get("viewing_trends_users").get(0));
      }
    }
  }

  @SuppressWarnings("unused") // We run through each table's iterator, but don't
  // need the values.
  private void verify() throws IOException {
	// Start
	try(FileTableReader<Phase0Presentations> in = new FileTableReader<Phase0Presentations>(Phase0Presentations.class,
		outFormat, presentationsFile)) {
	  log.info("Verifying file " + presentationsFile);	
	  for (final Phase0Presentations i : in ) {
	  }
	}
	try(FileTableReader<Phase0ViewingTrends> in = new FileTableReader<Phase0ViewingTrends>(Phase0ViewingTrends.class,
			outFormat, vtrendsFile)) {
		  log.info("Verifying file " + vtrendsFile);	
	  for (final Phase0ViewingTrends i : in ) {
	  }
	}
	try(FileTableReader<Phase0ViewingTrendsUser> in = new FileTableReader<Phase0ViewingTrendsUser>(Phase0ViewingTrendsUser.class,
			outFormat, vtrendsusersFile)) {
		  log.info("Verifying file " + vtrendsusersFile);	
	  for (final Phase0ViewingTrendsUser i : in ) {
	  }
	}
	// End
  }

  private void cleanup() {
    if (originalFile != null && originalFile.exists()) {
      originalFile.delete();
    }
    // STart
    if (presentationsFile != null && presentationsFile.exists()) {
    	presentationsFile.delete();
      }    
    if (vtrendsFile != null && vtrendsFile.exists()) {
    	vtrendsFile.delete();
      }
    if (vtrendsusersFile != null && vtrendsusersFile.exists()) {
    	vtrendsusersFile.delete();
      }    
  }

}
