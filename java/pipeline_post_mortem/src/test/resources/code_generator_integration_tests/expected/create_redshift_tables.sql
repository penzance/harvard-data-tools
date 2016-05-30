CREATE TABLE all_possible_column_types (
    big_int_column BIGINT,
    boolean_column BOOLEAN,
    date_column TIMESTAMP,
    timestamp_without_time_zone_column TIMESTAMP,
    datetime_column TIMESTAMP,
    double_precision_column DOUBLE PRECISION,
    int_column INTEGER,
    integer_column INTEGER,
    guid_column VARCHAR(50),
    text_column VARCHAR(256),
    timestamp_column TIMESTAMP,
    varchar_column VARCHAR(256),
    character_varying_column VARCHAR(256)
);

CREATE TABLE like_table (
    int_column INTEGER,
    string_column VARCHAR(256)
);

CREATE TABLE like_table_with_additions (
    int_column INTEGER,
    string_column VARCHAR(256),
    second_int_column INTEGER
);

CREATE TABLE simple_table (
    int_column INTEGER,
    string_column VARCHAR(256)
);

CREATE TABLE table_with_identifier (
    identifier_column VARCHAR(256)
);

CREATE TABLE table_with_multiplexed_identifier (
    multiplexed_identifier_column VARCHAR(256)
);
