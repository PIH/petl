drop temporary table if exists temp_encounter_types;
create temporary table temp_encounter_types
(
    uuid CHAR(38),
    name VARCHAR(100),
    description VARCHAR(1000),
    message VARCHAR(1000)
);

-- If there is not a previous watermark, initialize with all encounter types
insert into temp_encounter_types (uuid, name, description, message)
select uuid, name, description, 'initial-load' from encounter_type
where @previousWatermark is null;

-- If there is a previous watermark, initialize with only those encounter types since that watermark
insert into temp_encounter_types (uuid, name, description, message)
select t.uuid, t.name, t.description, 'incremental-load' from encounter_type t
inner join encounter_type_changes c on t.uuid = c.uuid
where @previousWatermark is not null
and c.last_updated >= @previousWatermark
and c.last_updated <= @newWatermark;

select uuid, name, description, message from temp_encounter_types;
