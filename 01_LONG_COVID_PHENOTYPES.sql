--************************************************************************************************
-- Script:        01_LONG_COVID_PHENOTYPES.sql
-- SAIL project:  1151 - Wales Multi-morbidity cohort - Census Data
-- HDR project:   HDR30 - Clinical coding of Long COVID-19 in Wales

-- About:         Long and acute COVID phenotype in primary and secondary care data
-- Author:        Hoda Abbasizanjani
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Long COVID phenotype for primary care data
-- ***********************************************************************************************
CREATE TABLE SAILW1151V.HDR30_PHEN_READ_LONG_COVID19 (
    name               char(20),
    code               char(12),
    desc               char(255),
    vendor_system      char(7),
    category           char(35),
    is_latest          char(1),
    valid_from         date,
    valid_to           date
    )
DISTRIBUTE BY HASH(code);

--DROP TABLE SAILW1151V.HDR30_PHEN_READ_LONG_COVID19;
--TRUNCATE TABLE SAILW1151V.HDR30_PHEN_READ_LONG_COVID19 IMMEDIATE;

INSERT INTO SAILW1151V.HDR30_PHEN_READ_LONG_COVID19 (name, vendor_system, category, desc, code)
VALUES
-- Vision
('Long COVID','Vision','Diagnostic codes','Post-COVID-19 syndrome','AyuJC'),
('Long COVID','Vision','Referral codes','Signposting to Your COVID Recovery','8HkjG'),
('Long COVID','Vision','Referral codes','Referral to post-COVID assessment clinic','8HTE6'),
('Long COVID','Vision','Referral codes','Referral to Your COVID Recovery rehabilitation platform','8HkI.'),
('Long COVID','Vision','Assessment codes','Post-COVID-19 Functional Status Scale patient self-report final scale grade','38Vx.'),
('Long COVID','Vision','Assessment codes','Post-COVID-19 Functional Status Scale structured interview final scale grade','38Vy.'),
('Long COVID','Vision','Diagnostic codes','Ongoing symptomatic disease caused by severe acute respiratory syndrome coronavirus 2','A7955'),
-- EMIS
('Long COVID','EMIS','Diagnostic codes','Post-COVID-19 syndrome','^ESCT1348645'),
('Long COVID','EMIS','Referral codes','Signposting to Your COVID Recovery','^ESCT1348624'),
('Long COVID','EMIS','Referral codes','Referral to post-COVID assessment clinic','^ESCT1348625'),
('Long COVID','EMIS','Referral codes','Referral to Your COVID Recovery rehabilitation platform','^ESCT1348626'),
('Long COVID','EMIS','Assessment codes','Newcastle post-COVID syndrome Follow-up Screening Questionnaire','^ESCT1348627'),
('Long COVID','EMIS','Assessment codes','Assessment using Newcastle post-COVID syndrome Follow-up Screening Questionnaire','^ESCT1348628'),
('Long COVID','EMIS','Assessment codes','C19-YRS (COVID-19 Yorkshire Rehabilitation Screening) tool','^ESCT1348629'),
('Long COVID','EMIS','Assessment codes','Assessment using C19-YRS (COVID-19 Yorkshire Rehabilitation Screening) tool','^ESCT1348631'),
('Long COVID','EMIS','Assessment codes','PCFS (Post-COVID-19 Functional Status) Scale patient self-report','^ESCT1348633'),
('Long COVID','EMIS','Assessment codes','Assessment using PCFS (Post-COVID-19 Functional Status) Scale patient self-report','^ESCT1348635'),
('Long COVID','EMIS','Assessment codes','PCFS (Post-COVID-19 Functional Status) Scale patient self-report final scale grade','^ESCT1348637'),
('Long COVID','EMIS','Assessment codes','PCFS (Post-COVID-19 Functional Status) Scale structured interview final scale grade','^ESCT1348639'),
('Long COVID','EMIS','Assessment codes','Assessment using PCFS (Post-COVID-19 Functional Status) Scale structured interview','^ESCT1348641'),
('Long COVID','EMIS','Assessment codes','PCFS (Post-COVID-19 Functional Status) Scale structured interview','^ESCT1348643'),
('Long COVID','EMIS','Diagnostic codes','Ongoing symptomatic COVID-19','^ESCT1348648');


UPDATE SAILW1151V.HDR30_PHEN_READ_LONG_COVID19
SET is_latest = '1',
    valid_from = '2023-05-26'
WHERE code IS NOT NULL;

SELECT * FROM SAILW1151V.HDR30_PHEN_READ_LONG_COVID19;

-- ***********************************************************************************************
-- Acute COVID phenotype in primary care data
-- ***********************************************************************************************
CREATE TABLE SAILW1151V.HDR30_PHEN_READ_ACUTE_COVID19 (
    name               char(20),
    code               char(12),
    desc               char(255),
    vendor_system      char(7),
    category           char(35),
    is_latest          char(1),
    valid_from         date,
    valid_to           date
    )
DISTRIBUTE BY HASH(code);

--DROP TABLE SAILW1151V.HDR30_PHEN_READ_ACUTE_COVID19;
--TRUNCATE TABLE SAILW1151V.HDR30_PHEN_READ_ACUTE_COVID19 IMMEDIATE;

INSERT INTO SAILW1151V.HDR30_PHEN_READ_ACUTE_COVID19 (name, vendor_system, category, desc, code)
VALUES
-- Read v2
('Acute COVID','Vision','Diagnostic codes','Acute disease caused by severe acute respiratory syndrome coronavirus 2 infections','A7954'),
('Acute COVID','Vision','Diagnostic codes','Encephalopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)','F289.'),
('Acute COVID','Vision','Diagnostic codes','Gastroenteritis caused by severe acute respiratory syndrome coronavirus 2 (disorder)','A0764'),
('Acute COVID','Vision','Diagnostic codes','Infection of upper respiratory tract caused by severe acute respiratory syndrome coronavirus 2 (disorder)','H0511'),
('Acute COVID','Vision','Diagnostic codes','Myocarditis caused by severe acute respiratory syndrome coronavirus 2 (disorder)','G5208'),
('Acute COVID','Vision','Diagnostic codes','Otitis media caused by severe acute respiratory syndrome coronavirus 2 (disorder)','F529.'),
('Acute COVID','Vision','Diagnostic codes','Cardiomyopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)','G5585'),
('Acute COVID','Vision','Diagnostic codes','Pneumonia caused by severe acute respiratory syndrome coronavirus 2 (disorder)','H204.'),
-- EMIS
('Acute COVID','EMIS','Diagnostic codes','Acute COVID-19 infection','^ESCT1348646'),
('Acute COVID','EMIS','Diagnostic codes','Acute respiratory distress syndrome due to disease caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348666'),
('Acute COVID','EMIS','Diagnostic codes','Lower respiratory infection caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348674'),
('Acute COVID','EMIS','Diagnostic codes','Otitis media caused by 2019-nCoV (novel coronavirus)','^ESCT1299056'),
('Acute COVID','EMIS','Diagnostic codes','Myocarditis caused by 2019-nCoV (novel coronavirus)','^ESCT1299059'),
('Acute COVID','EMIS','Diagnostic codes','Upper respiratory tract infection caused by 2019-nCoV (novel coronavirus)','^ESCT1299062'),
('Acute COVID','EMIS','Diagnostic codes','Pneumonia caused by 2019-nCoV (novel coronavirus)','^ESCT1299065'),
('Acute COVID','EMIS','Diagnostic codes','Encephalopathy caused by 2019-nCoV (novel coronavirus)','^ESCT1299068'),
('Acute COVID','EMIS','Diagnostic codes','Gastroenteritis caused by 2019-nCoV (novel coronavirus)','^ESCT1299071'),
('Acute COVID','EMIS','Diagnostic codes','Acute bronchitis caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348651'),
('Acute COVID','EMIS','Diagnostic codes','Cardiomyopathy caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1303237'),
('Acute COVID','EMIS','Diagnostic codes','Lymphocytopenia due to SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348480'),
('Acute COVID','EMIS','Diagnostic codes','Thrombocytopenia due to SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348487'),
('Acute COVID','EMIS','Diagnostic codes','Sepsis due to disease caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348516'),
('Acute COVID','EMIS','Diagnostic codes','Acute kidney injury due to disease caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348521'),
('Acute COVID','EMIS','Diagnostic codes','Acute hypoxemic respiratory failure due to disease caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348525'),
('Acute COVID','EMIS','Diagnostic codes','Rhabdomyolysis due to disease caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348529'),
('Acute COVID','EMIS','Diagnostic codes','Conjunctivitis due to disease caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348590'),
('Acute COVID','EMIS','Diagnostic codes','Dyspnoea caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348599'),
('Acute COVID','EMIS','Diagnostic codes','Fever caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)','^ESCT1348595'),
('Acute COVID','EMIS','Diagnostic codes','SARS-CoV-2 viraemia','^ESCT1416481');


UPDATE SAILW1151V.HDR30_PHEN_READ_ACUTE_COVID19
SET is_latest = '1',
    valid_from = '2023-05-26'
WHERE code IS NOT NULL;

SELECT * FROM SAILW1151V.HDR30_PHEN_READ_ACUTE_COVID19;

-- ***********************************************************************************************
-- Long COVID phenotype for secondary care data
-- ***********************************************************************************************
CREATE TABLE SAILW1151V.HDR30_PHEN_ICD10_LONG_COVID19 (
    name               char(20),
    code               char(5),
    desc               char(255),
    desc_ukhd          char(255),
    category           char(20),
    is_latest          char(1),
    valid_from         date,
    valid_to           date
    )
DISTRIBUTE BY HASH(code);

--DROP TABLE SAILW1151V.HDR30_PHEN_ICD10_LONG_COVID19;
--TRUNCATE TABLE SAILW1151V.HDR30_PHEN_ICD10_LONG_COVID19 IMMEDIATE;

INSERT INTO SAILW1151V.HDR30_PHEN_ICD10_LONG_COVID19 (code, desc, category)
VALUES ('U074','Post COVID-19 condition or symptom','Diagnostic codes');

UPDATE SAILW1151V.HDR30_PHEN_ICD10_LONG_COVID19
SET name = 'LONG_COVID19',
    is_latest = '1',
    valid_from = '2023-05-15'
WHERE code IS NOT NULL;

UPDATE SAILW1151V.HDR30_PHEN_ICD10_LONG_COVID19 tgt
SET desc_ukhd = src.description
FROM SAILUKHDV.ICD10_CODES_AND_TITLES_AND_METADATA src
WHERE src.icd_version LIKE '%5th%'
AND (tgt.code = src.alt_code OR tgt.code = src.code);

SELECT * FROM SAILW1151V.HDR30_PHEN_ICD10_LONG_COVID19;

