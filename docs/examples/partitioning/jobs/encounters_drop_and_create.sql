EXEC dbo.DROP_TABLE_IF_EXISTS 'encounters_${partition_num}';
EXEC dbo.CREATE_ENCOUNTERS_TABLE '_${partition_num}';
