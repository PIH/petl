
create table encounter_type (
    uuid CHAR(38),
    name VARCHAR(100),
    description VARCHAR(1000)
);

INSERT INTO encounter_type (uuid, name, description) VALUES ('55a0d3ea-a4d7-4e88-8f01-5aceb2d3c61b', 'Inscription', 'Check-in encounter, formerly known as Primary care reception');
INSERT INTO encounter_type (uuid, name, description) VALUES ('f1c286d0-b83f-4cd4-8348-7ea3c28ead13', 'Rencontre de paiement', 'Encounter used to capture patient payments');
INSERT INTO encounter_type (uuid, name, description) VALUES ('1373cf95-06e8-468b-a3da-360ac1cf026d', 'Consultation soins de base', 'Primary care visit (In Kreyol, it&apos;s &apos;vizit swen primè&apos;)');
INSERT INTO encounter_type (uuid, name, description) VALUES ('c4941dee-7a9b-4c1c-aa6f-8193e9e5e4e5', 'Post-operative note', 'The surgeons'' notes after performing surgery.');
INSERT INTO encounter_type (uuid, name, description) VALUES ('4fb47712-34a6-40d2-8ed3-e153abbd25b7', 'Signes vitaux', 'Encounter where vital signs were captured, and triage may have been done, possibly for triage purposes, but a complete exam was not done.');
INSERT INTO encounter_type (uuid, name, description) VALUES ('873f968a-73a8-4f9c-ac78-9f4778b751b6', 'Enregistrement de patient', 'Patient registration -- normally a new patient');
