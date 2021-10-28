
CREATE FUNCTION concept_name(
    _conceptID INT,
    _locale varchar(50)
)
    RETURNS VARCHAR(255)
    DETERMINISTIC

BEGIN
    DECLARE conceptName varchar(255);

    SELECT name INTO conceptName
    FROM concept_name
    WHERE voided = 0
      AND concept_id = _conceptID
    order by if(_locale = locale, 0, 1), if(locale = 'en', 0, 1),
             locale_preferred desc, ISNULL(concept_name_type) asc,
             field(concept_name_type,'FULLY_SPECIFIED','SHORT')
    limit 1;

    RETURN conceptName;
END

#

CREATE FUNCTION encounter_type(
    _name_or_uuid varchar(255)
)
    RETURNS INT
    DETERMINISTIC

BEGIN
    DECLARE ret varchar(255);

    SELECT  encounter_type_id INTO ret
    FROM    encounter_type where name = _name_or_uuid or uuid = _name_or_uuid;

    RETURN ret;
END

#