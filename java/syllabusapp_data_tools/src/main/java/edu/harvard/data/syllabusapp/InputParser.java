package edu.harvard.data.syllabusapp;

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
import edu.harvard.data.syllabusapp.SyllabusappDataConfig;
import edu.harvard.data.syllabusapp.bindings.phase0.Phase0Syllabus;
import edu.harvard.data.syllabusapp.bindings.phase0.Phase0SyllabusBody;
import edu.harvard.data.syllabusapp.bindings.phase0.Phase0SyllabusLink;
import edu.harvard.data.syllabusapp.bindings.phase0.Phase0SyllabusNameLookup;
import edu.harvard.data.syllabusapp.bindings.phase0.Phase0SyllabusDelta;
import edu.harvard.data.syllabusapp.bindings.phase0.Phase0SyllabusFiles;
import edu.harvard.data.pipeline.InputTableIndex;

public class InputParser {

  private static final Logger log = LogManager.getLogger();

  private final SyllabusappDataConfig config;
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
  private final S3ObjectId syllabusOutputDir;
  private final S3ObjectId sylbodyOutputDir;
  private final S3ObjectId syllinkOutputDir;
  private final S3ObjectId sylnamelookupEnrollOutputDir;
  private final S3ObjectId syldeltaOutputDir;
  private final S3ObjectId sylfilesOutputDir;


  public InputParser(final SyllabusappDataConfig config, final AwsUtils aws,
      final S3ObjectId inputObj, final S3ObjectId outputLocation) {
    this.config = config;
    this.aws = aws;
    this.inputObj = inputObj;
    this.key = inputObj.getKey();
	this.filename = key.substring(key.lastIndexOf("/") + 1);	  
    this.dataproductPrefix = "PrepCanvasRest_";
    this.dataproductFiletype = ".json.gz";
    this.currentDataProduct = getDataProduct();
    this.syllabusOutputDir = AwsUtils.key(outputLocation, "Syllabus");
    this.sylbodyOutputDir = AwsUtils.key(outputLocation, "SyllabusBody");
    this.syllinkOutputDir = AwsUtils.key(outputLocation, "SyllabusLink");
    this.sylnamelookupEnrollOutputDir = AwsUtils.key( outputLocation, "SyllabusNameLookup" );
    this.syldeltaOutputDir = AwsUtils.key( outputLocation, "SyllabusDelta" );
    this.sylfilesOutputDir = AwsUtils.key( outputLocation, "SyllabusFiles" );
    final FormatLibrary formatLibrary = new FormatLibrary();
    this.inFormat = formatLibrary.getFormat(Format.Sis);
    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.setSerializationInclusion(Include.NON_NULL);    
    this.inFormat.setJsonMapper(jsonMapper);
    this.outFormat = formatLibrary.getFormat(Format.DecompressedRest);
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
    
    if (currentDataProduct.equals("Syllabus") ) {
        dataproductOutputObj = AwsUtils.key(syllabusOutputDir, dataproductFilename );  
    } else if ( currentDataProduct.equals("SyllabusBody") ) {
        dataproductOutputObj = AwsUtils.key(sylbodyOutputDir, dataproductFilename);        
    } else if ( currentDataProduct.equals("SyllabusLink") ) {
        dataproductOutputObj = AwsUtils.key(syllinkOutputDir, dataproductFilename);   
    } else if ( currentDataProduct.equals("SyllabusNameLookup") ) {
        dataproductOutputObj = AwsUtils.key(sylnamelookupEnrollOutputDir, dataproductFilename);
    } else if ( currentDataProduct.equals("SyllabusDelta") ) {
        dataproductOutputObj = AwsUtils.key(syldeltaOutputDir, dataproductFilename);
    } else if ( currentDataProduct.equals("SyllabusFiles") ) {
        dataproductOutputObj = AwsUtils.key(sylfilesOutputDir, dataproductFilename);
    }
    
    log.info("Parsing " + filename + " to " + dataproductFile);
    log.info("DataProduct Key: " + dataproductOutputObj );
    
  }

  private void parse() throws IOException {
    log.info("Parsing file " + originalFile);
    if (currentDataProduct.equals("Syllabus")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0Syllabus> syllabi = new TableWriter<Phase0Syllabus>(Phase0Syllabus.class, outFormat,
    	            dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  syllabi.add((Phase0Syllabus) tables.get("Syllabus").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("SyllabusBody")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0SyllabusBody> sylbody = new TableWriter<Phase0SyllabusBody>(Phase0SyllabusBody.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  sylbody.add((Phase0SyllabusBody) tables.get("SyllabusBody").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("SyllabusLink")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0SyllabusLink> syllink = new TableWriter<Phase0SyllabusLink>(Phase0SyllabusLink.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  syllink.add((Phase0SyllabusLink) tables.get("SyllabusLink").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("SyllabusNameLookup")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0SyllabusNameLookup> sylnamelookup = new TableWriter<Phase0SyllabusNameLookup>(Phase0SyllabusNameLookup.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  sylnamelookup.add((Phase0SyllabusNameLookup) tables.get("SyllabusNameLookup").get(0));
    		}
    	}			
	} else if (currentDataProduct.equals("SyllabusDelta")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0SyllabusDelta> syldelta = new TableWriter<Phase0SyllabusDelta>(Phase0SyllabusDelta.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    		      syldelta.add((Phase0SyllabusDelta) tables.get("SyllabusDelta").get(0));
    		}
    	}			
	} else if (currentDataProduct.equals("SyllabusFiles")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0SyllabusFiles> sylfiles = new TableWriter<Phase0SyllabusFiles>(Phase0SyllabusFiles.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    		      sylfiles.add((Phase0SyllabusFiles) tables.get("SyllabusFiles").get(0));
    		}
    	}			
	}
    log.info("Done Parsing file " + originalFile);
  }

  @SuppressWarnings("unused") // We run through each table's iterator, but don't
  // need the values.
  private void verify() throws IOException {
    if (currentDataProduct.equals("Syllabus")) {

	    try(FileTableReader<Phase0Syllabus> in = new FileTableReader<Phase0Syllabus>(Phase0Syllabus.class,
		    outFormat, dataproductFile)) {
	      log.info("Verifying file " + dataproductFile);	
	      for (final Phase0Syllabus i : in ) {
	      }
	    }
    } else if (currentDataProduct.equals("SyllabusBody")) {
	    try(FileTableReader<Phase0SyllabusBody> in = new FileTableReader<Phase0SyllabusBody>(Phase0SyllabusBody.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0SyllabusBody i : in ) {
	      }
	    }	    
    } else if (currentDataProduct.equals("SyllabusLink")) {
	    try(FileTableReader<Phase0SyllabusLink> in = new FileTableReader<Phase0SyllabusLink>(Phase0SyllabusLink.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);
	      for (final Phase0SyllabusLink i : in ) {
	      }
	    }
    } else if (currentDataProduct.equals("SyllabusNameLookup")) {
	    try(FileTableReader<Phase0SyllabusNameLookup> in = new FileTableReader<Phase0SyllabusNameLookup>(Phase0SyllabusNameLookup.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0SyllabusNameLookup i : in ) {
	      }
	    }
    } else if (currentDataProduct.equals("SyllabusDelta")) {
	    try(FileTableReader<Phase0SyllabusDelta> in = new FileTableReader<Phase0SyllabusDelta>(Phase0SyllabusDelta.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0SyllabusDelta i : in ) {
	      }
	    }
    } else if (currentDataProduct.equals("SyllabusFiles")) {
	    try(FileTableReader<Phase0SyllabusFiles> in = new FileTableReader<Phase0SyllabusFiles>(Phase0SyllabusFiles.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0SyllabusFiles i : in ) {
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
