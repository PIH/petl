
create table if not exists etl_status (
    uuid            CHAR(36) NOT NULL,
    num             INT NOT NULL,
    job_name        VARCHAR(100) NOT NULL,
    total_expected  INT,
    total_loaded    INT,
    started         DATETIME NOT NULL,
    completed       DATETIME,
    status          VARCHAR(1000) NOT NULL,
    error_message   VARCHAR(1000)
)
;
