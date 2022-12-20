CREATE TABLE encounters (
    id uuid PRIMARY KEY,
    server_id text,
    encounter_id int,
    encounter_type int,
    patient_id int,
    location_id int,
    form_id int,
    encounter_datetime timestamp,
    creator int,
    date_created timestamp,
    voided int,
    voided_by int,
    date_voided timestamp,
    void_reason text,
    changed_by int,
    date_changed timestamp,
    visit_id int
);
