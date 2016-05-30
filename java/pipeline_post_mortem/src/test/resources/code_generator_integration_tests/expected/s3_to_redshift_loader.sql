-- This file was automatically generated. Do not manually edit.
-- See http://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html
-- for Redshift update strategies.


------- Table all_possible_column_types-------
TRUNCATE all_possible_column_types;
VACUUM all_possible_column_types;
ANALYZE all_possible_column_types;
COPY all_possible_column_types (big_int_column,boolean_column,date_column,timestamp_without_time_zone_column,datetime_column,double_precision_column,int_column,integer_column,guid_column,text_column,timestamp_column,varchar_column,character_varying_column) FROM '<intermediates3bucketandpath>/all_possible_column_types/' CREDENTIALS '<awskeyandsecret>' DELIMITER '\t' TRUNCATECOLUMNS GZIP;
VACUUM all_possible_column_types;
ANALYZE all_possible_column_types;

------- Table like_table-------
TRUNCATE like_table;
VACUUM like_table;
ANALYZE like_table;
COPY like_table (int_column,string_column) FROM '<intermediates3bucketandpath>/like_table/' CREDENTIALS '<awskeyandsecret>' DELIMITER '\t' TRUNCATECOLUMNS GZIP;
VACUUM like_table;
ANALYZE like_table;

------- Table like_table_with_additions-------
TRUNCATE like_table_with_additions;
VACUUM like_table_with_additions;
ANALYZE like_table_with_additions;
COPY like_table_with_additions (int_column,string_column,second_int_column) FROM '<intermediates3bucketandpath>/like_table_with_additions/' CREDENTIALS '<awskeyandsecret>' DELIMITER '\t' TRUNCATECOLUMNS GZIP;
VACUUM like_table_with_additions;
ANALYZE like_table_with_additions;

------- Table simple_table-------
TRUNCATE simple_table;
VACUUM simple_table;
ANALYZE simple_table;
COPY simple_table (int_column,string_column) FROM '<intermediates3bucketandpath>/simple_table/' CREDENTIALS '<awskeyandsecret>' DELIMITER '\t' TRUNCATECOLUMNS GZIP;
VACUUM simple_table;
ANALYZE simple_table;

------- Table table_with_identifier-------
TRUNCATE table_with_identifier;
VACUUM table_with_identifier;
ANALYZE table_with_identifier;
COPY table_with_identifier (identifier_column) FROM '<intermediates3bucketandpath>/table_with_identifier/' CREDENTIALS '<awskeyandsecret>' DELIMITER '\t' TRUNCATECOLUMNS GZIP;
VACUUM table_with_identifier;
ANALYZE table_with_identifier;

------- Table table_with_multiplexed_identifier-------
TRUNCATE table_with_multiplexed_identifier;
VACUUM table_with_multiplexed_identifier;
ANALYZE table_with_multiplexed_identifier;
COPY table_with_multiplexed_identifier (multiplexed_identifier_column) FROM '<intermediates3bucketandpath>/table_with_multiplexed_identifier/' CREDENTIALS '<awskeyandsecret>' DELIMITER '\t' TRUNCATECOLUMNS GZIP;
VACUUM table_with_multiplexed_identifier;
ANALYZE table_with_multiplexed_identifier;
