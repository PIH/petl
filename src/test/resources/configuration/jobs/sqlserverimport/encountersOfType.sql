select uuid, name, description
from encounter_type
where name like concat('%',@encounterType,'%');
