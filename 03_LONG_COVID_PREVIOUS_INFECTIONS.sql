--************************************************************************************************
-- Script:       03_LONG_COVID_PREVIOUS_INFECTIONS.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
-- HDR project:  HDR30 - Clinical coding of Long COVID-19 in Wales

-- About:        Extract date of all COVID-19 infections
-- Author:       Hoda Abbasizanjani
-- ***********************************************************************************************
-- ***********************************************************************************************
-- All COVID infections
-- ***********************************************************************************************
CREATE TABLE SAILW1151V.HDR30_ALL_CONFIRMED_COVID (
    alf_e                               bigint,
    covid19_confirmed_date              date,
    data_source                         char(4)
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILW1151V.HDR30_ALL_CONFIRMED_COVID;
--TRUNCATE TABLE SAILW1151V.HDR30_ALL_CONFIRMED_COVID IMMEDIATE;

-------------------------------------------------------------------------------------------------
-- Insert positive COVID PCR tests from PATD
-------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_ALL_CONFIRMED_COVID
    SELECT alf_e, date(spcm_collected_dt), 'PATD'
    FROM SAILWMC_V.C19_COHORT_PATD_DF_COVID_LIMS_TESTRESULTS v
    WHERE spcm_collected_dt IS NOT NULL
    AND alf_e IS NOT NULL
    AND covid19testresult = 'Positive'
    AND spcm_collected_dt >= '2020-01-01'
    AND spcm_collected_dt <= CURRENT date
;

-------------------------------------------------------------------------------------------------
-- Insert positive lateral flow tests from CVLF
-------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_ALL_CONFIRMED_COVID
    SELECT alf_e, appt_dt, 'CVLF'
    FROM SAILWMC_V.C19_COHORT_CVLF_DF_LATERAL_FLOW_TESTS
    WHERE testresult IN ('SCT:1240581000000104', 'SCT:1322781000000102')
    AND alf_e IS NOT NULL
    AND appt_dt >= '2020-01-01'
    AND appt_dt <= CURRENT date
;

-------------------------------------------------------------------------------------------------
-- Insert confirmed diagnosis of COVID recorded in PEDW
-------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_ALL_CONFIRMED_COVID
    SELECT alf_e, admis_dt, 'PEDW'
    FROM SAILWMC_V.C19_COHORT_PEDW_SPELL s
    INNER JOIN SAILWMC_V.C19_COHORT_PEDW_EPISODE e
    ON s.prov_unit_cd = e.prov_unit_cd
    AND s.spell_num_e = e.spell_num_e
    INNER JOIN SAILWMC_V.C19_COHORT_PEDW_DIAG d
    ON e.prov_unit_cd = d.prov_unit_cd
    AND e.spell_num_e = d.spell_num_e
    AND e.epi_num = d.epi_num
    WHERE d.diag_cd_1234 = 'U071'
    AND alf_e IS NOT NULL
    AND admis_dt >= '2020-01-01'
    AND admis_dt <= CURRENT date
;

-------------------------------------------------------------------------------------------------
-- Insert confirmed diagnosis of COVID recorded in WLGP
-------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_ALL_CONFIRMED_COVID
    SELECT alf_e, event_dt, 'WLGP'
    FROM SAILWMC_V.C19_COHORT_WLGP_GP_EVENT_CLEANSED v
    WHERE event_cd IN ('A7953','4J3R.','A7952','A7951','4J3R1')
    AND alf_e IS NOT NULL
    AND event_dt >= '2020-01-01'
    AND event_dt <= CURRENT date
;

-- ***********************************************************************************************
-- Basic checks
-- ***********************************************************************************************
SELECT data_source, count(*), count(DISTINCT alf_e)
FROM SAILW1151V.HDR30_ALL_CONFIRMED_COVID
GROUP BY data_source;

SELECT count(DISTINCT alf_e) FROM SAILW1151V.HDR30_ALL_CONFIRMED_COVID;

SELECT count(DISTINCT alf_e) FROM SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION
WHERE alf_e IN (SELECT alf_e FROM SAILW1151V.HDR30_ALL_CONFIRMED_COVID);
-------------------------------------------------------------------------------------------------
