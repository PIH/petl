create table encounter_types (
  uuid CHAR(38),
  name VARCHAR(100),
  description VARCHAR(1000),
  import_date DATETIME,
  import_reason VARCHAR(100)
);
