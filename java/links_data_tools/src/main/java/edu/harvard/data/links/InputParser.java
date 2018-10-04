package edu.harvard.data.links;

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
import edu.harvard.data.links.LinksDataConfig;
import edu.harvard.data.links.bindings.phase0.Phase0ScraperText;
import edu.harvard.data.links.bindings.phase0.Phase0ScraperCitations;
import edu.harvard.data.links.bindings.phase0.Phase0OpenScholarMapping;
import edu.harvard.data.links.bindings.phase0.Phase0OpenScholarBiblioTitles;
import edu.harvard.data.links.bindings.phase0.Phase0OpenScholarPages;
import edu.harvard.data.pipeline.InputTableIndex;

public class InputParser {

  private static final Logger log = LogManager.getLogger();

  private final LinksDataConfig config;
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
  private final S3ObjectId scraperOutputDir;
  private final S3ObjectId citationOutputDir;
  private final S3ObjectId ostitlesOutputDir;
  private final S3ObjectId osmappingOutputDir;
  private final S3ObjectId ospagesOutputDir;


  public InputParser(final LinksDataConfig config, final AwsUtils aws,
      final S3ObjectId inputObj, final S3ObjectId outputLocation) {
    this.config = config;
    this.aws = aws;
    this.inputObj = inputObj;
    this.key = inputObj.getKey();
	this.filename = key.substring(key.lastIndexOf("/") + 1);	  
    this.dataproductPrefix = "PrepLinks_";
    this.dataproductFiletype = ".json.gz";
    this.currentDataProduct = getDataProduct();
    this.scraperOutputDir = AwsUtils.key(outputLocation, "ScraperText");
    this.citationOutputDir = AwsUtils.key(outputLocation, "ScraperCitations");
    this.osmappingOutputDir = AwsUtils.key(outputLocation, "OpenScholarMapping");
    this.ostitlesOutputDir = AwsUtils.key(outputLocation, "OpenScholarBiblioTitles");
    this.ospagesOutputDir = AwsUtils.key(outputLocation, "OpenScholarPages");
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
    
    if (currentDataProduct.equals("ScraperText") ) {
        dataproductOutputObj = AwsUtils.key(scraperOutputDir, dataproductFilename );  
    } else if ( currentDataProduct.equals("ScraperCitations") ) {
        dataproductOutputObj = AwsUtils.key(citationOutputDir, dataproductFilename);        
    } else if ( currentDataProduct.equals("OpenScholarMapping") ) {
        dataproductOutputObj = AwsUtils.key(osmappingOutputDir , dataproductFilename);        
    } else if ( currentDataProduct.equals("OpenScholarBiblioTitles") ) {
        dataproductOutputObj = AwsUtils.key(ostitlesOutputDir, dataproductFilename);        
    } else if ( currentDataProduct.equals("OpenScholarPages") ) {
        dataproductOutputObj = AwsUtils.key(ospagesOutputDir, dataproductFilename);        
    }
    
    log.info("Parsing " + filename + " to " + dataproductFile);
    log.info("DataProduct Key: " + dataproductOutputObj );
    
  }

  private void parse() throws IOException {
    log.info("Parsing file " + originalFile);
    if (currentDataProduct.equals("ScraperText")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0ScraperText> scraped = new TableWriter<Phase0ScraperText>(Phase0ScraperText.class, outFormat,
    	            dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  scraped.add((Phase0ScraperText) tables.get("ScraperText").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("ScraperCitations")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0ScraperCitations> citations = new TableWriter<Phase0ScraperCitations>(Phase0ScraperCitations.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  citations.add((Phase0ScraperCitations) tables.get("ScraperCitations").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("OpenScholarMapping")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0OpenScholarMapping> citations = new TableWriter<Phase0OpenScholarMapping>(Phase0OpenScholarMapping.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  citations.add((Phase0OpenScholarMapping) tables.get("OpenScholarMapping").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("OpenScholarBiblioTitles")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0OpenScholarBiblioTitles> citations = new TableWriter<Phase0OpenScholarBiblioTitles>(Phase0OpenScholarBiblioTitles.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  citations.add((Phase0OpenScholarBiblioTitles) tables.get("OpenScholarBiblioTitles").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("OpenScholarPages")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0OpenScholarPages> citations = new TableWriter<Phase0OpenScholarPages>(Phase0OpenScholarPages.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  citations.add((Phase0OpenScholarPages) tables.get("OpenScholarPages").get(0));
    		}
    	}
	}
    log.info("Done Parsing file " + originalFile);
  }

  @SuppressWarnings("unused") // We run through each table's iterator, but don't
  // need the values.
  private void verify() throws IOException {
    if (currentDataProduct.equals("ScraperText")) {

	    try(FileTableReader<Phase0ScraperText> in = new FileTableReader<Phase0ScraperText>(Phase0ScraperText.class,
		    outFormat, dataproductFile)) {
	      log.info("Verifying file " + dataproductFile);	
	      for (final Phase0ScraperText i : in ) {
	      }
	    }
    } else if (currentDataProduct.equals("ScraperCitations")) {
	    try(FileTableReader<Phase0ScraperCitations> in = new FileTableReader<Phase0ScraperCitations>(Phase0ScraperCitations.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0ScraperCitations i : in ) {
	      }
	    }	    
    } else if (currentDataProduct.equals("OpenScholarMapping")) {
	    try(FileTableReader<Phase0OpenScholarMapping> in = new FileTableReader<Phase0OpenScholarMapping>(Phase0OpenScholarMapping.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0OpenScholarMapping i : in ) {
	      }
	    }	    
    } else if (currentDataProduct.equals("OpenScholarBiblioTitles")) {
	    try(FileTableReader<Phase0OpenScholarBiblioTitles> in = new FileTableReader<Phase0OpenScholarBiblioTitles>(Phase0OpenScholarBiblioTitles.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0OpenScholarBiblioTitles i : in ) {
	      }
	    }	    
    } else if (currentDataProduct.equals("OpenScholarPages")) {
	    try(FileTableReader<Phase0OpenScholarPages> in = new FileTableReader<Phase0OpenScholarPages>(Phase0OpenScholarPages.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0OpenScholarPages i : in ) {
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
