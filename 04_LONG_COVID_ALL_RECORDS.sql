--************************************************************************************************
-- Script:       04_LONG_COVID_ALL_RECORDS.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
-- HDR project:  HDR30 - Clinical coding of Long COVID-19 in Wales

-- About:        Extract all Long COVID records
-- Author:       Hoda Abbasizanjani
-- ***********************************************************************************************
-- ***********************************************************************************************
CREATE TABLE SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS (
    alf_e                          bigint,
    long_covid_data_source         char(5),
    long_covid_date                date,
    long_covid_code                char(12),
    long_covid_code_desc           char(255),
    long_covid_code_type           char(12)
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS;
--TRUNCATE TABLE SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS IMMEDIATE;

------------------------------------------------------------------------------------------------
-- Insert long COVID records from WLGP
------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS
    SELECT alf_e,
           'WLGP',
           event_dt,
           event_cd,
           event_cd_description,
           event_cd_type
    FROM SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID
    WHERE event_cd_phen = 'Long COVID' -- only LC, and not including acute COVID
;
------------------------------------------------------------------------------------------------
-- Insert long COVID records from PEDW
------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS
    SELECT s.alf_e,
           'PEDW', 
           s.admis_dt,
           code,
           desc,
           'ICD-10'
    FROM SAILWMCCV.C19_COHORT_PEDW_SPELL s
    INNER JOIN SAILWMCCV.C19_COHORT_PEDW_EPISODE e
    ON s.prov_unit_cd = e.prov_unit_cd
    AND s.spell_num_e = e.spell_num_e
    INNER JOIN SAILWMCCV.C19_COHORT_PEDW_DIAG d
    ON e.prov_unit_cd = d.prov_unit_cd
    AND e.spell_num_e = d.spell_num_e
    AND e.epi_num = d.epi_num
    INNER JOIN SAILW1151V.HDR30_PHEN_ICD10_LONG_COVID19
    ON d.diag_cd_1234 = code
    WHERE s.admis_dt >= '2020-01-01' AND s.admis_dt <= CURRENT date
;

------------------------------------------------------------------------------------------------
-- Insert long COVID records from OPDW
------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS
    SELECT o.alf_e,
           'OPDW',
           o.attend_dt,
           code,
           desc,
           'ICD-10'
    FROM SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS o
    INNER JOIN SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS_DIAG d
    ON o.prov_unit_cd = d.prov_unit_cd
    AND o.case_rec_num_e = d.case_rec_num_e
    AND o.att_id_e = d.att_id_e
    AND o.attend_dt = d.attend_dt
    INNER JOIN SAILW1151V.HDR30_PHEN_ICD10_LONG_COVID19
    ON CONCAT(diag_cd_123, diag_cd_4) = code
    WHERE o.attend_dt >= '2020-01-01' AND o.attend_dt <= CURRENT date
;

 -- ***********************************************************************************************
-- Basic checks
-- ***********************************************************************************************
SELECT long_covid_data_source, count(*), count(DISTINCT alf_e)
FROM SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS
GROUP BY long_covid_data_source;

SELECT min(long_covid_date), max(long_covid_date) FROM SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS;

SELECT * FROM  SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS ORDER BY long_covid_date;

SELECT min(long_covid_date) FROM SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS WHERE long_covid_data_source = 'PEDW';

SELECT count(*), count(DISTINCT alf_e) FROM  SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS
WHERE long_covid_code_type = 'ICD-10'
AND YEAR(long_covid_date) IN (2020, 2021, 2022);
------------------------------------------------------------------------------------------------

