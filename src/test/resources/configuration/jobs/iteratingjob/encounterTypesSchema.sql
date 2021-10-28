IF OBJECT_ID('encounter_types') IS NULL
create table encounter_types (
    uuid CHAR(38),
    name VARCHAR(500),
    description VARCHAR(500)
);
