delete t from encounter_types_1 t
inner join encounter_type_changes c on t.uuid = c.uuid
where t.partition_num = 1
and c.last_updated >= @previousWatermark
and c.last_updated <= @newWatermark
;