
CREATE_ENCOUNTERS_TABLE '_temp'

IF (dbo.NUM_COLUMNS_CHANGED('encounters_temp', 'encounters') > 0)
BEGIN
  EXEC DROP_TABLE_IF_EXISTS 'encounters';
  EXEC CREATE_ENCOUNTERS_TABLE '';
END
;

DROP TABLE encounters_temp;
