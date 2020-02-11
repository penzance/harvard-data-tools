package edu.harvard.data.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.internal.S3DirectSpi;
import com.amazonaws.services.s3.model.S3ObjectId;

import edu.harvard.data.AwsUtils;
import edu.harvard.data.DataConfig;
import edu.harvard.data.FormatLibrary;
import edu.harvard.data.schema.DataSchemaColumn;
import edu.harvard.data.schema.DataSchemaTable;
import edu.harvard.data.schema.DataSchemaType;
import edu.harvard.data.schema.TableOwner;
import edu.harvard.data.schema.fulltext.FullTextSchema;
import edu.harvard.data.schema.fulltext.FullTextTable;

public class CreateHiveTableGenerator {

  private static final Logger log = LogManager.getLogger();

  private final GenerationSpec schemaVersions;
  private final File dir;
  private final FullTextSchema textSchema;
  private final DataConfig config;
  

  public CreateHiveTableGenerator(final File dir, final DataConfig config, 
		  final GenerationSpec schemaVersions, final FullTextSchema textSchema ) {
	this.config = config;
    this.dir = dir;
    this.schemaVersions = schemaVersions;
    this.textSchema = textSchema;
  }

  public void generate() throws IOException {
    for (int i=1; i<3; i++) {
      final String fileBase = "phase_" + (i+1) + "_create_tables";
      final File phaseFile = new File(dir, fileBase + ".sh");
      try (final PrintStream out = new PrintStream(new FileOutputStream(phaseFile))) {
        log.info("Creating Hive " + phaseFile + " file in " + dir);
        
        generateCreateTablesFile(out, i + 1, schemaVersions.getPhase(i), schemaVersions.getPhase(i + 1),
           config.getEmrLogDir() + "/" + fileBase + ".out");
      }
    }
  }

  private void generateCreateTablesFile(final PrintStream out, final int phase, final SchemaPhase input,
      final SchemaPhase output, final String logFile) {
    List<String> tableNames = new ArrayList<String>(input.getSchema().getTables().keySet());
	Collections.sort(tableNames);
	log.info("tablenames: " + tableNames );
    out.println("sudo mkdir -p /var/log/hive/user/hadoop # Workaround for Hive logging bug");
    out.println("sudo chown hive:hive -R /var/log/hive");
    generatePersistentTables(out, phase, input, "merged_", true, true, logFile, true );
    generatePersistentTables(out, phase, input, "cur_", true, false, logFile, true );
    generateDropStatements(out, phase, "in_", tableNames, input.getSchema().getTables(), logFile );
    out.println();
    generateDropStatements(out, phase, "out_", tableNames, output.getSchema().getTables(), logFile );
    out.println(); 
    generateCreateStatements(out, phase, input, "in_", true, logFile, false );
    generateCreateStatements(out, phase, output, "out_", false, logFile, false );
    out.println("exit $?");
  }
  
  private void generatePersistentTables(final PrintStream out, final int phase, final SchemaPhase currentPhase, 
		  final String prefix, final boolean ignoreOwner, final boolean isTransactional, final String logFile,
		  final boolean addMetadata ) {
  	  
  	final AwsUtils aws = new AwsUtils();  
	if (currentPhase != null) {
	  final Map<String, DataSchemaTable> inTables = currentPhase.getSchema().getTables();
	  final List<String> inTableKeys = new ArrayList<String>(inTables.keySet());
	  Collections.sort(inTableKeys);

      out.println("if ! hadoop fs -test -e " + "/current" + "; then ");
      out.println("echo \"Creating persistent tables...\"");      
	  for (final String tableKey : inTableKeys) {
	    final DataSchemaTable table = inTables.get(tableKey);
	    if (!(table.isTemporary() && table.getExpirationPhase() < phase)) {
	      if (ignoreOwner || (table.getOwner() != null && table.getOwner().equals(TableOwner.hive))) {
	        final String tableName = prefix + table.getTableName();
	        if (textSchema.tableNames().contains(table.getTableName() ) ) {
	            if ( isTransactional ) {
	                createTableTransactional( out, tableName, table, logFile, addMetadata );
	            } else {
	                final S3ObjectId fulltextobj = AwsUtils.key(config.getFullTextLocation(), table.getTableName() + "/fulltable");
	                if ( aws.keyExists(fulltextobj) ) {
	                    generateCopyStatement(out, tableName, table );
	                    createTablePartial( out, tableName, table, "/current", logFile, addMetadata );
	            	    out.println();
	                } else {
	                	out.println("echo \"Full text table for " + table.getTableName() + " does not exist.\"" + " >> " + logFile + " 2>&1");
	                }
	            }
	        }
	      }
	    }
	  }
      out.println("fi");
	}
	out.println();
  }

  private void generateDropStatements(final PrintStream out, final int phase, final String prefix,
      final List<String> tableNames, final Map<String, DataSchemaTable> tables, final String logFile ) {
	out.println("sudo hive -e \"");
	for (final String tableName : tableNames) {
      final DataSchemaTable table = tables.get(tableName);
        if (!(table.isTemporary() && table.getExpirationPhase() < phase)) {
          out.println("  DROP TABLE IF EXISTS " + prefix + table.getTableName() + " PURGE;");
        }
    }
	out.println("\" >> " + logFile + " 2>&1");
  }

  private void generateCreateStatements(final PrintStream out, final int phase, final SchemaPhase currentPhase,
      final String prefix, final boolean ignoreOwner, final String logFile, final boolean addMetadata ) {
    if (currentPhase != null) {
      final Map<String, DataSchemaTable> inTables = currentPhase.getSchema().getTables();
      final List<String> inTableKeys = new ArrayList<String>(inTables.keySet());
      Collections.sort(inTableKeys);

      for (final String tableKey : inTableKeys) {
        final DataSchemaTable table = inTables.get(tableKey);
        if (!(table.isTemporary() && table.getExpirationPhase() < phase)) {
          if (ignoreOwner || (table.getOwner() != null && table.getOwner().equals(TableOwner.hive))) {
            final String tableName = prefix + table.getTableName();
            out.println("if hadoop fs -test -e " + currentPhase.getHDFSDir() + "/" + table.getTableName() + "; then ");
            createTable(out, tableName, table, currentPhase.getHDFSDir(), logFile, addMetadata );
            out.println("fi");
          }
        }
      }
    }
  }
  
  private void generateCopyStatement( final PrintStream out, final String tableName,
		  final DataSchemaTable table ) {
	    out.println("hadoop fs -mkdir /current" + "/" + table.getTableName() );    	
        out.println("s3-dist-cp --src=" + AwsUtils.uri(config.getFullTextLocation())
    	 	      + "/" + table.getTableName() + "/fulltable"
    		      + " --dest=hdfs:///current" + "/" + table.getTableName() );
  }

  private void createTable(final PrintStream out, final String tableName,
      final DataSchemaTable table, final String locationVar, final String logFile, final boolean addMetadata ) {

      final FormatLibrary formatLibrary = new FormatLibrary();
      final boolean isQuotedFormat = formatLibrary.getFormat(config.getFulltextFormat())
						  .getCsvFormat()
						  .isQuoteCharacterSet();
	  
	out.println("sudo hive -e \"");
    out.println("  CREATE EXTERNAL TABLE " + tableName + " (");
    listFields(out, table, table.getListofColumns(), addMetadata );
    out.println("    )");
    //out.println("    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED By '\\n'");
    addRowFormat(out, false );
    out.println("    STORED AS TEXTFILE");
    out.println("    LOCATION '" + locationVar + "/" + table.getTableName() + "/';");
    out.println();
    out.println("\" >> " + logFile + " 2>&1");
  }

  private void createTablePartial(final PrintStream out, final String tableName,
	      final DataSchemaTable table, final String locationVar, final String logFile, final boolean addMetadata ) {
	
	final FormatLibrary formatLibrary = new FormatLibrary();
	final boolean isQuotedFormat = formatLibrary.getFormat(config.getFulltextFormat())
						    .getCsvFormat()
						    .isQuoteCharacterSet();
	
	final FullTextTable fulltexttable = textSchema.get( table.getTableName() );
	final List<String> textfieldsonly = fulltexttable.getColumns();
	textfieldsonly.add(0, fulltexttable.getKey());
	out.println("sudo hive -e \"");
	out.println("  CREATE EXTERNAL TABLE " + tableName + " (");
	listFields(out, table, textfieldsonly, addMetadata );
	out.println("    )");
	//out.println("    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED By '\\n'");
	addRowFormat(out, false );
	out.println("    STORED AS TEXTFILE");
	out.println("    LOCATION '" + locationVar + "/" + table.getTableName() + "/';");
	out.println();
	out.println("\" >> " + logFile + " 2>&1");
  }  
  
  private void createTableTransactional(final PrintStream out, final String tableName,
	      final DataSchemaTable table, final String logFile, final boolean addMetadata ) {
	final FullTextTable fulltexttable = textSchema.get( table.getTableName() );
	final List<String> textfieldsonly = fulltexttable.getColumns();
	textfieldsonly.add(0, fulltexttable.getKey());
	out.println("sudo hive -e \"");	
	out.println("  CREATE TABLE " + tableName + " (");
	listFields(out, table, textfieldsonly, addMetadata );
	out.println("    )");
	out.println("    COMMENT 'Latest comprehensive output data merging current + historical'");
	out.println("    CLUSTERED BY (" + fulltexttable.getKey() + ") into 2 buckets stored as orc");
	out.println("    TBLPROPERTIES ('transactional'='true');");
    out.println("\" >> " + logFile + " 2>&1");	
	out.println();
  }

  private void listFields(final PrintStream out, final DataSchemaTable table, 
		  final List<String> subsetcolumns, final boolean addMetadata ) {
	final FullTextTable fulltexttable = textSchema.get( table.getTableName() );
	final List<DataSchemaColumn> columns = table.getColumns();
    String concatFields = new String();
    List<String> listofstrings = new ArrayList<String>();
    List<String> listofmeta = new ArrayList<String>();
    String separator = ",\n";
	final DataSchemaType timestamptype = (DataSchemaType) DataSchemaType.parse("timestamp");
    for (int i = 0; i < columns.size(); i++) {
      final DataSchemaColumn column = columns.get(i);
      String columnName = column.getName();
      String newColumnName = new String();
      if (subsetcolumns.contains(columnName)) {
    	  newColumnName = checkField( columnName, column, true );
    	  listofstrings.add(newColumnName);
    	  if (addMetadata && !columnName.equals( fulltexttable.getKey()) ) {
    		  String timestampField = addTimestamp( columnName );
    		  listofmeta.add( addCheckedField(timestampField, timestamptype.getHiveType(), true ));
    	  }
      }
    }
    List<String> orderList = new ArrayList<String>(listofstrings);
    if (addMetadata) orderList.addAll(listofmeta);
    concatFields = StringUtils.join( orderList, separator );
    out.println(concatFields);
  }
  
  private String addTimestamp( final String columnName ) {
	  return ("time_" + columnName );
  }
  
  private String checkField( final String columnName, final DataSchemaColumn column,
		  final boolean protectAgainstReservedKeywords ) {
	String verifiedColumn = new String();
    if (columnName.contains(".")) verifiedColumn = columnName.substring(columnName.lastIndexOf(".") + 1);  
    else verifiedColumn = columnName;
    return addCheckedField( verifiedColumn, column.getType().getHiveType(), protectAgainstReservedKeywords);
  }
  
  private String addCheckedField( final String columnName, final String columnType,
		  final boolean protectAgainstReservedKeywords ) {
	if (protectAgainstReservedKeywords) {
	        return ("    " + "\\`" + columnName + "\\`" + " " + columnType);
	} else {
	    	return ("    " + columnName + " " + columnType);
	}
  }
  
  private void addRowFormat( final PrintStream out, final boolean quotedFields ) {
	  
	  if (quotedFields) {
		  out.println("    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'");
		  out.println("    WITH SERDEPROPERTIES (");
		  out.println("       'separatorChar' = '\\\\t',");
		  out.println("       'escapeChar' = '\\\\\\\\'");
		  out.println("    )");
	  } else {
		  out.println("    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' LINES TERMINATED By '\\n'");
	  }
  }
  
}
