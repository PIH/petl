
-- Create function to allow determination if a schema has changed

CREATE OR ALTER FUNCTION NUM_COLUMNS_CHANGED(@t1 VARCHAR(255), @t2 VARCHAR(255))
    RETURNS INT
    WITH EXECUTE AS CALLER
AS
BEGIN
    DECLARE @NumChanges INT;

    SET @NumChanges = (
        SELECT count(*)
        FROM sys.dm_exec_describe_first_result_set (N'SELECT * FROM ' + @t1, NULL, 0) t1
        FULL OUTER JOIN  sys.dm_exec_describe_first_result_set (N'SELECT * FROM ' + @t2, NULL, 0) t2 ON t1.name = t2.name
        WHERE ((isnull(t1.name, '') != isnull(t2.name, '')) OR (isnull(t1.system_type_name, '') != isnull(t2.system_type_name, '')))
    )
    ;

    RETURN(@NumChanges);
END
;

