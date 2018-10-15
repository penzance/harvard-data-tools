package edu.harvard.data.peoplesoft;

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
import edu.harvard.data.peoplesoft.PeoplesoftDataConfig;
import edu.harvard.data.peoplesoft.bindings.phase0.Phase0Appointments;
import edu.harvard.data.pipeline.InputTableIndex;

public class InputParser {

  private static final Logger log = LogManager.getLogger();

  private final PeoplesoftDataConfig config;
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
  private final S3ObjectId appointmentOutputDir;


  public InputParser(final PeoplesoftDataConfig config, final AwsUtils aws,
      final S3ObjectId inputObj, final S3ObjectId outputLocation) {
    this.config = config;
    this.aws = aws;
    this.inputObj = inputObj;
    this.key = inputObj.getKey();
	this.filename = key.substring(key.lastIndexOf("/") + 1);	  
    this.dataproductPrefix = "PrepPeoplesoft_";
    this.dataproductFiletype = ".json.gz";
    this.currentDataProduct = getDataProduct();
    this.appointmentOutputDir = AwsUtils.key(outputLocation, "Appointments");
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
    
    if (currentDataProduct.equals("Appointments") ) {
        dataproductOutputObj = AwsUtils.key(appointmentOutputDir, dataproductFilename );  
    } 
    
    log.info("Parsing " + filename + " to " + dataproductFile);
    log.info("DataProduct Key: " + dataproductOutputObj );
    
  }

  private void parse() throws IOException {
    log.info("Parsing file " + originalFile);
    if (currentDataProduct.equals("Appointments")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0Appointments> appointments = new TableWriter<Phase0Appointments>(Phase0Appointments.class, outFormat,
    	            dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  appointments.add((Phase0Appointments) tables.get("Appointments").get(0));
    		}
    	}
	}
    log.info("Done Parsing file " + originalFile);
  }

  @SuppressWarnings("unused") // We run through each table's iterator, but don't
  // need the values.
  private void verify() throws IOException {
    if (currentDataProduct.equals("Appointments")) {

	    try(FileTableReader<Phase0Appointments> in = new FileTableReader<Phase0Appointments>(Phase0Appointments.class,
		    outFormat, dataproductFile)) {
	      log.info("Verifying file " + dataproductFile);	
	      for (final Phase0Appointments i : in ) {
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
