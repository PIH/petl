CREATE OR ALTER PROCEDURE dbo.CREATE_ENCOUNTERS_TABLE
    @tableSuffix VARCHAR(255)
AS
BEGIN
    DECLARE @CREATE_SQL NVARCHAR(1000);

    SET @CREATE_SQL =
        'CREATE TABLE dbo.encounters' + @tableSuffix + ' (' +
        '   site		  VARCHAR(50),' +
        '   patient_id           INT,' +
        '   encounter_id	  INT,' +
        '   encounter_datetime   DATETIME,' +
        '   encounter_type       VARCHAR(100),' +
        '   location    	  VARCHAR(100),' +
        '   date_created         DATETIME,' +
        '   partition_num	  INT' +
        ') ' +
        'ON psSite(partition_num);';
    ;

    EXEC(@CREATE_SQL);
END;

