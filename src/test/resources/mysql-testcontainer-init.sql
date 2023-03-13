
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
INSERT INTO encounter_type (uuid, name, description) VALUES ('1b3d1e13-f0b1-4b83-86ea-b1b1e2fb4efa', 'Commande de radio', 'Radiology Order  - the ordering of a radiology exam');
INSERT INTO encounter_type (uuid, name, description) VALUES ('92fd09b4-5335-4f7e-9f63-b2a663fd09a6', 'Consultation', 'Encounter where a full or abbreviated examination is done, leading to a presumptive or confirmed diagnosis');
INSERT INTO encounter_type (uuid, name, description) VALUES ('5b1b4a4e-0084-4137-87db-dba76c784439', 'Examen de radiologie', 'Radiology Study - represents performance of a radiology study on a patient by a radiology technician');
INSERT INTO encounter_type (uuid, name, description) VALUES ('d5ca53a7-d3b5-44ac-9aa2-1491d2a4b4e9', 'Rapport de radiologie', 'Radiology Report - represents a report on a radiology study performed by a radiologist');
INSERT INTO encounter_type (uuid, name, description) VALUES ('b6631959-2105-49dd-b154-e1249e0fbcd7', 'Sortie de soins hospitaliers', 'Indicates that a patient&apos;s inpatient care at the hospital is ending, and they are expected to leave soon');
INSERT INTO encounter_type (uuid, name, description) VALUES ('260566e1-c909-4d61-a96f-c1019291a09d', 'Admission aux soins hospitaliers', 'Indicates that the patient has been admitted for inpatient care, and is not expected to leave the hospital unless discharged.');
INSERT INTO encounter_type (uuid, name, description) VALUES ('436cfe33-6b81-40ef-a455-f134a9f7e580', 'Transfert', 'Indicates that a patient is being transferred into a different department within the hospital. (Transfers out of the hospital should not use this encounter type.)');
INSERT INTO encounter_type (uuid, name, description) VALUES ('8ff50dea-18a1-4609-b4c9-3f8f2d611b84', 'Médicaments administrés', 'When someone gets medicine from the pharmacy');
INSERT INTO encounter_type (uuid, name, description) VALUES ('edbb857b-e736-4296-9438-462b31f97ef9', 'Annuler l''admission', 'An encounter that notes that a request to admit a patient (via giving them a dispositon of &quot;admit&quot; on another form) is being overridden');
INSERT INTO encounter_type (uuid, name, description) VALUES ('1545d7ff-60f1-485e-9c95-5740b8e6634b', 'Death Certificate', 'The official record of a patient''s death. A patient may be dead without having one of these encounters, but no living patient should have an encounter with this type');
INSERT INTO encounter_type (uuid, name, description) VALUES ('ffa148de-2c88-4828-833e-f3788991543d', 'Antecedents', 'Past medical history, for general primary care. Typically only captured at a patient''s first visit');
INSERT INTO encounter_type (uuid, name, description) VALUES ('0a9facff-fdc4-4aa9-aae0-8d7feaf5b3ef', 'Examen', 'Physical exam, typically captured at every clinical visit');
INSERT INTO encounter_type (uuid, name, description) VALUES ('e0aaa214-1d4b-442a-b527-144adf025299', 'Conduite a tenir', 'Orders placed during a consultation');
INSERT INTO encounter_type (uuid, name, description) VALUES ('035fb8da-226a-420b-8d8b-3904f3bedb25', 'Consultation d''oncologie', 'Consultation for oncology');
INSERT INTO encounter_type (uuid, name, description) VALUES ('09febbd8-03f1-11e5-8418-1697f925ec7b', 'Diagnostic', 'Diagnosis, typically captured at every clinical visit');
INSERT INTO encounter_type (uuid, name, description) VALUES ('4d77916a-0620-11e5-a6c0-1697f925ec7b', 'Laboratory Results', 'Laboratory Results');
INSERT INTO encounter_type (uuid, name, description) VALUES ('f9cfdf8b-d086-4658-9b9d-45a62896da03', 'Oncology initial visit', 'Intake for oncology patient');
INSERT INTO encounter_type (uuid, name, description) VALUES ('828964fa-17eb-446e-aba4-e940b0f4be5b', 'Chemotheraphy treatment session', 'Chemotheraphy treatment session for HUM and other places.');
INSERT INTO encounter_type (uuid, name, description) VALUES ('ae06d311-1866-455b-8a64-126a9bd74171', 'NCD Initial Consult', 'Non-communicable disease initial consult');
INSERT INTO encounter_type (uuid, name, description) VALUES ('5C16E1D6-8E73-47E4-A861-D6AAC03E2224', 'Primary Care Disposition', 'Indicates the disposition of the primary care visit');
INSERT INTO encounter_type (uuid, name, description) VALUES ('92DBE011-67CA-4C0C-80DB-D38989E554C9', 'Primary Care Pediatric Feeding', 'Indicates the current feeding');
INSERT INTO encounter_type (uuid, name, description) VALUES ('D25FFD97-417F-46CC-85EE-3E7DA68B0D07', 'Primary Care Pediatric Supplements', 'Indicates the supplements taken by a pediatric patient');
INSERT INTO encounter_type (uuid, name, description) VALUES ('a8584ab8-cc2a-11e5-9956-625662870761', 'Mental Health Consult', 'Mental health visit and assessment');
INSERT INTO encounter_type (uuid, name, description) VALUES ('5b812660-0262-11e6-a837-0800200c9a66', 'Primary Care Pediatric Initial Consult', 'Primary Care Pediatric Initial Consult');
INSERT INTO encounter_type (uuid, name, description) VALUES ('229e5160-031b-11e6-a837-0800200c9a66', 'Primary Care Pediatric Followup Consult', 'Primary Care Pediatric Followup Consult');
INSERT INTO encounter_type (uuid, name, description) VALUES ('27d3a180-031b-11e6-a837-0800200c9a66', 'Primary Care Adult Initial Consult', 'Primary Care Adult Initial Consult');
INSERT INTO encounter_type (uuid, name, description) VALUES ('27d3a181-031b-11e6-a837-0800200c9a66', 'Primary Care Adult Followup Consult', 'Primary Care Adult Followup Consult');
INSERT INTO encounter_type (uuid, name, description) VALUES ('74cef0a6-2801-11e6-b67b-9e71128cae77', 'Emergency Triage', 'Emergency Department patient triage');
INSERT INTO encounter_type (uuid, name, description) VALUES ('b3a0e3ad-b80c-4f3f-9626-ace1ced7e2dd', 'Test Order', 'Test Order - the order of test (labs, biopsy, etc)');
INSERT INTO encounter_type (uuid, name, description) VALUES ('10db3139-07c0-4766-b4e5-a41b01363145', 'Pathology Specimen Collection', 'Pathology Specimen Collection - the collection of a pathology specimen for a test (blood draw, biopsy, etc)');
INSERT INTO encounter_type (uuid, name, description) VALUES ('5cbfd6a2-92d9-4ad0-b526-9d29bfe1d10c', 'NCD Followup Consult', 'Non-communicable disease followup consult');
INSERT INTO encounter_type (uuid, name, description) VALUES ('c31d306a-40c4-11e7-a919-92ebcb67fe33', 'ZL VIH Données de Base', 'ZL VIH Données de Base (HIV Intake)');
INSERT INTO encounter_type (uuid, name, description) VALUES ('c31d3312-40c4-11e7-a919-92ebcb67fe33', 'ZL VIH Rendez-vous', 'ZL VIH Rendez-vous (HIV Followup)');
INSERT INTO encounter_type (uuid, name, description) VALUES ('c31d3416-40c4-11e7-a919-92ebcb67fe33', 'ZL VIH Données de Base Pédiatriques', 'VIH Données de Base Pédiatriques (HIV intake child)');
INSERT INTO encounter_type (uuid, name, description) VALUES ('c31d34f2-40c4-11e7-a919-92ebcb67fe33', 'ZL VIH Rendez-vous Pédiatriques', 'Fiche de suivi des visites cliniques - Enfants VIH+ (HIV followup child)');
INSERT INTO encounter_type (uuid, name, description) VALUES ('17536ba6-dd7c-4f58-8014-08c7cb798ac7', 'Saisie Première pour le VIH', 'iSantePlus Saisie Première visite Adulte VIH');
INSERT INTO encounter_type (uuid, name, description) VALUES ('204ad066-c5c2-4229-9a62-644bc5617ca2', 'Suivi Visite pour le VIH', 'iSantePlus Saisie visite suivi Adulte VIH');
INSERT INTO encounter_type (uuid, name, description) VALUES ('349ae0b4-65c1-4122-aa06-480f186c8350', 'Saisie Première pour le VIH (pédiatrique)', 'iSantePlus Saisie Première visite Pédiatrique VIH');
INSERT INTO encounter_type (uuid, name, description) VALUES ('33491314-c352-42d0-bd5d-a9d0bffc9bf1', 'Suivi visite pour le VIH (pédiatrique)', 'iSantePlus Saisie visite Suivi pédiatrique VIH');
INSERT INTO encounter_type (uuid, name, description) VALUES ('c45d7299-ad08-4cb5-8e5d-e0ce40532939', 'ART Adhérence', 'iSantePlus Conseils ART d''Adhérence');
INSERT INTO encounter_type (uuid, name, description) VALUES ('5021b1a1-e7f6-44b4-ba02-da2f2bcf8718', 'Visit Document Upload', 'Encounters used to record visit documents complex obs.');
INSERT INTO encounter_type (uuid, name, description) VALUES ('616b66fe-f189-11e7-8c3f-9a214cf093ae', 'Voluntary counselling and testing', 'Voluntary counselling and testing (VCT) for HIV');
INSERT INTO encounter_type (uuid, name, description) VALUES ('de844e58-11e1-11e8-b642-0ed5f89f718b', 'Socio-economics', 'Education and housing information on the patient');
INSERT INTO encounter_type (uuid, name, description) VALUES ('00e5e810-90ec-11e8-9eb6-529269fb1459', 'ANC Intake', 'Initial prenatal (aka ANC) visit for pregnant mother');
INSERT INTO encounter_type (uuid, name, description) VALUES ('00e5e946-90ec-11e8-9eb6-529269fb1459', 'ANC Followup', 'Followup prenatal (aka ANC) visits for pregnant mother');
INSERT INTO encounter_type (uuid, name, description) VALUES ('00e5ebb2-90ec-11e8-9eb6-529269fb1459', 'MCH Delivery', 'Mother''s visit for delivery of baby');
INSERT INTO encounter_type (uuid, name, description) VALUES ('8ad0b7d3-2973-40ae-916c-0369f3df86c5', 'Clinic Initial Visit', 'First visit to one of our primary care clinics');
INSERT INTO encounter_type (uuid, name, description) VALUES ('b29cec8c-b21c-4c95-bfed-916a51db2a26', 'Clinic Visit', 'A visit to one of our primary care clinics');
INSERT INTO encounter_type (uuid, name, description) VALUES ('aa61d509-6e76-4036-a65d-7813c0c3b752', 'Consult', 'A doctor consult at one of our primary care clinics');
INSERT INTO encounter_type (uuid, name, description) VALUES ('39C09928-0CAB-4DBA-8E48-39C631FA4286', 'Lab Specimen Collection', 'Lab Specimen Collection - the collection of a lab specimen for a test (blood draw, biopsy, etc)');
INSERT INTO encounter_type (uuid, name, description) VALUES ('1e2a509c-7c9f-11e9-8f9e-2a86e4085a59', 'Vaccination', 'Vaccination form only (not within another encounter)');
INSERT INTO encounter_type (uuid, name, description) VALUES ('fdee591e-78ba-11e9-8f9e-2a86e4085a59', 'Echocardiogram', 'Echocardiogram consultation');
INSERT INTO encounter_type (uuid, name, description) VALUES ('91DDF969-A2D4-4603-B979-F2D6F777F4AF', 'Prenatal Home Assessment', 'Prenatal home assessment');
INSERT INTO encounter_type (uuid, name, description) VALUES ('0CF4717A-479F-4349-AE6F-8602E2AA41D3', 'Pediatric Home Assessment', 'Pediatric home assessment');
INSERT INTO encounter_type (uuid, name, description) VALUES ('0E7160DF-2DD1-4728-B951-641BBE4136B8', 'Maternal Post-partum Home Assessment', 'Maternal post-partum home assessment');
INSERT INTO encounter_type (uuid, name, description) VALUES ('690670E2-A0CC-452B-854D-B95E2EAB75C9', 'Maternal Follow-up Home Assessment', 'Maternal follow-up home assessment');

create table encounter_type_changes (
    uuid char(38),
    last_updated datetime(3)
);

insert into encounter_type_changes (uuid, last_updated) values ('aa61d509-6e76-4036-a65d-7813c0c3b752', '2022-02-04 10:32:15.012'); -- Consult
insert into encounter_type_changes (uuid, last_updated) values ('55a0d3ea-a4d7-4e88-8f01-5aceb2d3c61b', '2022-02-04 22:11:19.556'); -- Inscription
insert into encounter_type_changes (uuid, last_updated) values ('1e2a509c-7c9f-11e9-8f9e-2a86e4085a59', '2022-02-05 09:54:09.112'); -- Vaccination