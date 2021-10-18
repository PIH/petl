
TRUNCATE TABLE encounters WITH (PARTITIONS (${partition_num}));
ALTER TABLE encounters_${partition_num} SWITCH PARTITION ${partition_num} TO encounters PARTITION ${partition_num};
DROP TABLE encounters_${partition_num};
