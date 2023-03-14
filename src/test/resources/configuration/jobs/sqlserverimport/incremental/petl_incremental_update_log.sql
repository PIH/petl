CREATE TABLE petl_incremental_update_log
(
    table_name         varchar(100),
    partition_num      int,
    starting_watermark datetime,
    ending_watermark   datetime,
    completed_datetime datetime
);