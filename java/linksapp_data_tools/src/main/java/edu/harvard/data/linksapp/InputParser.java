package edu.harvard.data.linksapp;

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
import edu.harvard.data.linksapp.LinksappDataConfig;
import edu.harvard.data.linksapp.bindings.phase0.Phase0ScraperText;
import edu.harvard.data.linksapp.bindings.phase0.Phase0ScraperTextEntity;
import edu.harvard.data.linksapp.bindings.phase0.Phase0ScraperCitations;
import edu.harvard.data.linksapp.bindings.phase0.Phase0ScraperCitationsCatalyst;
import edu.harvard.data.linksapp.bindings.phase0.Phase0OpenScholarMapping;
import edu.harvard.data.linksapp.bindings.phase0.Phase0OpenScholarBiblioTitles;
import edu.harvard.data.linksapp.bindings.phase0.Phase0OpenScholarPages;
import edu.harvard.data.linksapp.bindings.phase0.Phase0Gazette;
import edu.harvard.data.linksapp.bindings.phase0.Phase0GazetteEvents;
import edu.harvard.data.linksapp.bindings.phase0.Phase0Rss;
import edu.harvard.data.linksapp.bindings.phase0.Phase0HiltIntoPractice;
import edu.harvard.data.pipeline.InputTableIndex;

public class InputParser {

  private static final Logger log = LogManager.getLogger();

  private final LinksappDataConfig config;
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
  private final S3ObjectId entityOutputDir;
  private final S3ObjectId citationOutputDir;
  private final S3ObjectId catalystOutputDir;
  private final S3ObjectId ostitlesOutputDir;
  private final S3ObjectId osmappingOutputDir;
  private final S3ObjectId ospagesOutputDir;
  private final S3ObjectId gazetteOutputDir;
  private final S3ObjectId geventsOutputDir;
  private final S3ObjectId rssOutputDir;
  private final S3ObjectId hiltipOutputDir;


  public InputParser(final LinksappDataConfig config, final AwsUtils aws,
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
    this.entityOutputDir = AwsUtils.key(outputLocation, "ScraperTextEntity");
    this.citationOutputDir = AwsUtils.key(outputLocation, "ScraperCitations");
    this.catalystOutputDir = AwsUtils.key(outputLocation, "ScraperCitationsCatalyst");
    this.osmappingOutputDir = AwsUtils.key(outputLocation, "OpenScholarMapping");
    this.ostitlesOutputDir = AwsUtils.key(outputLocation, "OpenScholarBiblioTitles");
    this.ospagesOutputDir = AwsUtils.key(outputLocation, "OpenScholarPages");
    this.gazetteOutputDir = AwsUtils.key(outputLocation, "Gazette");
    this.geventsOutputDir = AwsUtils.key(outputLocation, "GazetteEvents");
    this.rssOutputDir = AwsUtils.key(outputLocation, "Rss");
    this.hiltipOutputDir = AwsUtils.key(outputLocation, "HiltIntoPractice");
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
    } else if ( currentDataProduct.equals("ScraperTextEntity") ) {
        dataproductOutputObj = AwsUtils.key(entityOutputDir, dataproductFilename);        
    } else if ( currentDataProduct.equals("ScraperCitations") ) {
        dataproductOutputObj = AwsUtils.key(citationOutputDir, dataproductFilename);        
    } else if ( currentDataProduct.equals("ScraperCitationsCatalyst") ) {
        dataproductOutputObj = AwsUtils.key(catalystOutputDir, dataproductFilename);        
    } else if ( currentDataProduct.equals("OpenScholarMapping") ) {
        dataproductOutputObj = AwsUtils.key(osmappingOutputDir , dataproductFilename);        
    } else if ( currentDataProduct.equals("OpenScholarBiblioTitles") ) {
        dataproductOutputObj = AwsUtils.key(ostitlesOutputDir, dataproductFilename);        
    } else if ( currentDataProduct.equals("OpenScholarPages") ) {
        dataproductOutputObj = AwsUtils.key(ospagesOutputDir, dataproductFilename);        
    } else if ( currentDataProduct.equals("Gazette") ) {
        dataproductOutputObj = AwsUtils.key(gazetteOutputDir, dataproductFilename);        
    } else if ( currentDataProduct.equals("GazetteEvents") ) {
        dataproductOutputObj = AwsUtils.key(geventsOutputDir, dataproductFilename);        
    } else if ( currentDataProduct.equals("Rss") ) {
        dataproductOutputObj = AwsUtils.key(rssOutputDir, dataproductFilename);        
    } else if ( currentDataProduct.equals("HiltIntoPractice") ) {
        dataproductOutputObj = AwsUtils.key(hiltipOutputDir, dataproductFilename);        
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
	} else if (currentDataProduct.equals("ScraperTextEntity")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0ScraperTextEntity> entities = new TableWriter<Phase0ScraperTextEntity>(Phase0ScraperTextEntity.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  entities.add((Phase0ScraperTextEntity) tables.get("ScraperTextEntity").get(0));
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
	} else if (currentDataProduct.equals("ScraperCitationsCatalyst")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0ScraperCitationsCatalyst> citations = new TableWriter<Phase0ScraperCitationsCatalyst>(Phase0ScraperCitationsCatalyst.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  citations.add((Phase0ScraperCitationsCatalyst) tables.get("ScraperCitationsCatalyst").get(0));
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
	} else if (currentDataProduct.equals("Gazette")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0Gazette> articles = new TableWriter<Phase0Gazette>(Phase0Gazette.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  articles.add((Phase0Gazette) tables.get("Gazette").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("GazetteEvents")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0GazetteEvents> gevents = new TableWriter<Phase0GazetteEvents>(Phase0GazetteEvents.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  gevents.add((Phase0GazetteEvents) tables.get("GazetteEvents").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("Rss")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0Rss> rssfeeds = new TableWriter<Phase0Rss>(Phase0Rss.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  rssfeeds.add((Phase0Rss) tables.get("Rss").get(0));
    		}
    	}
	} else if (currentDataProduct.equals("HiltIntoPractice")) {
        log.info("Parsing data product " + currentDataProduct);
    	try (
    	        final JsonFileReader in = new JsonFileReader(inFormat, originalFile,
    	            new EventJsonDocumentParser(inFormat, true, currentDataProduct));
    	    	TableWriter<Phase0HiltIntoPractice> hiltips = new TableWriter<Phase0HiltIntoPractice>(Phase0HiltIntoPractice.class, outFormat,
    	                dataproductFile);) {
    		for (final Map<String, List<? extends DataTable>> tables : in) {
    			  hiltips.add((Phase0HiltIntoPractice) tables.get("HiltIntoPractice").get(0));
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
    } else if (currentDataProduct.equals("ScraperTextEntity")) {

	    try(FileTableReader<Phase0ScraperTextEntity> in = new FileTableReader<Phase0ScraperTextEntity>(Phase0ScraperTextEntity.class,
		    outFormat, dataproductFile)) {
	      log.info("Verifying file " + dataproductFile);	
	      for (final Phase0ScraperTextEntity i : in ) {
	      }
	    }
    } else if (currentDataProduct.equals("ScraperCitations")) {
	    try(FileTableReader<Phase0ScraperCitations> in = new FileTableReader<Phase0ScraperCitations>(Phase0ScraperCitations.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0ScraperCitations i : in ) {
	      }
	    }	    
    } else if (currentDataProduct.equals("ScraperCitationsCatalyst")) {
	    try(FileTableReader<Phase0ScraperCitationsCatalyst> in = new FileTableReader<Phase0ScraperCitationsCatalyst>(Phase0ScraperCitationsCatalyst.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0ScraperCitationsCatalyst i : in ) {
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
    } else if (currentDataProduct.equals("Gazette")) {
	    try(FileTableReader<Phase0Gazette> in = new FileTableReader<Phase0Gazette>(Phase0Gazette.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0Gazette i : in ) {
	      }
	    }	    
    } else if (currentDataProduct.equals("GazetteEvents")) {
	    try(FileTableReader<Phase0GazetteEvents> in = new FileTableReader<Phase0GazetteEvents>(Phase0GazetteEvents.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0GazetteEvents i : in ) {
	      }
	    }	    
    } else if (currentDataProduct.equals("Rss")) {
	    try(FileTableReader<Phase0Rss> in = new FileTableReader<Phase0Rss>(Phase0Rss.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0Rss i : in ) {
	      }
	    }	    
    } else if (currentDataProduct.equals("HiltIntoPractice")) {
	    try(FileTableReader<Phase0HiltIntoPractice> in = new FileTableReader<Phase0HiltIntoPractice>(Phase0HiltIntoPractice.class,
			outFormat, dataproductFile)) {
		  log.info("Verifying file " + dataproductFile);	
	      for (final Phase0HiltIntoPractice i : in ) {
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
