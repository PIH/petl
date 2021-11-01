
IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = N'pfSite')
BEGIN
CREATE PARTITION FUNCTION pfSite(INT) AS RANGE LEFT FOR VALUES (
    1,2,3,4,5,6,7,8,9,10
);
END
;

-- Create partition scheme with this function
IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = N'psSite')
BEGIN
CREATE PARTITION SCHEME psSite
    AS PARTITION pfSite
    ALL TO ([Primary]);
END
;
