select '${site}' as site, e.patient_id, e.encounter_id, e.encounter_datetime, et.name as encounter_type, l.name as location, e.date_created, ${partition_num} as partition_num
from encounter e
inner join encounter_type et on e.encounter_type = et.encounter_type_id
inner join location l on e.location_id = l.location_id
and l.name = '${locationName}';
