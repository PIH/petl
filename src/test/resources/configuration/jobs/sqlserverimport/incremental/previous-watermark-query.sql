select  max(l.ending_watermark)
from    petl_incremental_update_log l
where   l.table_name = 'encounter_types'
and     l.partition_num = 1
;
