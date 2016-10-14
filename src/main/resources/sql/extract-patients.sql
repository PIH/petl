/*
  Extracts basic attributes of a patient that are directly accessible from the patient and person tables
 */
select      p.patient_id,
            n.uuid as patient_uuid,
            n.gender,
            n.birthdate,
            n.birthdate_estimated,
            n.birthtime,
            n.dead,
            n.death_date,
            n.cause_of_death
from        patient p
inner join  person n on p.patient_id = n.person_id