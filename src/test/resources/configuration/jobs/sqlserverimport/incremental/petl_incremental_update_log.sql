CREATE TABLE petl_incremental_update_log
(
    table_name         varchar(100),
    partition_num      int,
    starting_watermark datetime2(3),
    ending_watermark   datetime2(3),
    completed_datetime datetime2(3)
);