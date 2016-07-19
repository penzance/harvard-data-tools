sudo mkdir -p /var/log/hive/user/hadoop # Workaround for Hive logging bug
sudo chown hive:hive -R /var/log/hive
hive -e "
  DROP TABLE IF EXISTS in_all_possible_column_types PURGE;
  DROP TABLE IF EXISTS in_table_with_identifier PURGE;
  DROP TABLE IF EXISTS in_table_with_multiplexed_identifier PURGE;
  DROP TABLE IF EXISTS out_all_possible_column_types PURGE;
  DROP TABLE IF EXISTS out_table_with_identifier PURGE;
  DROP TABLE IF EXISTS out_table_with_multiplexed_identifier PURGE;
  CREATE EXTERNAL TABLE in_all_possible_column_types (
    big_int_column BIGINT,
    boolean_column BOOLEAN,
    date_column DATE,
    timestamp_without_time_zone_column DATE,
    datetime_column TIMESTAMP,
    double_precision_column DOUBLE,
    int_column INT,
    integer_column INT,
    guid_column STRING,
    text_column STRING,
    timestamp_column TIMESTAMP,
    varchar_column STRING,
    character_varying_column STRING
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED By '\n'
  STORED AS TEXTFILE
  LOCATION 'hdfs_1/all_possible_column_types/';
  CREATE EXTERNAL TABLE in_table_with_identifier (
    identifier_column STRING
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED By '\n'
  STORED AS TEXTFILE
  LOCATION 'hdfs_1/table_with_identifier/';
  CREATE EXTERNAL TABLE in_table_with_multiplexed_identifier (
    multiplexed_identifier_column STRING
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED By '\n'
  STORED AS TEXTFILE
  LOCATION 'hdfs_1/table_with_multiplexed_identifier/';
" &> /home/hadoop/phase_2_create_tables.out
exit $?
