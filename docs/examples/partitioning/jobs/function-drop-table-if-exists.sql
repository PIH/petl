
-- Create utility procedure to drop a table if it exists

CREATE OR ALTER PROCEDURE DROP_TABLE_IF_EXISTS
    @tableName VARCHAR(255)
AS
BEGIN
    DECLARE @DROP_SQL NVARCHAR(1000);
    
    IF OBJECT_ID(@tableName) IS NOT NULL
    BEGIN
    	SET @DROP_SQL = 'DROP TABLE ' + @tableName;
    	EXEC(@DROP_SQL);
    END
END;


