--************************************************************************************************
-- Script:        02_LONG_COVID_PRIMARY_CARE.sql
-- SAIL project:  1151 - Wales Multi-morbidity cohort - Census Data
-- HDR project:   HDR30 - Clinical coding of Long COVID-19 in Wales

-- About:         Long COVID recording in primary care data (WLGP)
-- Author:        Hoda Abbasizanjani
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Long and acute COVID recording in primary care data (WLGP)
-- ***********************************************************************************************
CREATE TABLE SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID LIKE SAILWMC_V.C19_COHORT_WLGP_GP_EVENT_CLEANSED;

--DROP TABLE SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID;
--TRUNCATE TABLE SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID IMMEDIATE;

-------------------------------------------------------------------------------------------------
-- Add additional columns
-------------------------------------------------------------------------------------------------
ALTER TABLE SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID ADD event_cd_phen char(12) NULL;
ALTER TABLE SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID ADD event_cd_type char(7) NULL;
ALTER TABLE SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID ADD event_cd_full char(12) NULL;
ALTER TABLE SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID ADD event_cd_description char(255) NULL;
ALTER TABLE SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID ADD event_cd_category char(100) NULL;
ALTER TABLE SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID ADD record_order int NULL;

-------------------------------------------------------------------------------------------------
-- Insert Long COVID recordings from WLGP for EMIS practices
-------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID
    SELECT g.*,
           c.name AS event_cd_phen,
           c.vendor_system AS event_cd_type,
           c.code AS event_cd_full,
           c.desc AS event_cd_description,
           c.category AS event_cd_category,
           ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY event_dt) AS record_order
    FROM SAILWMC_V.C19_COHORT_WLGP_GP_EVENT_CLEANSED g
    INNER JOIN SAILW1151V.HDR30_PHEN_READ_LONG_COVID19 c
    ON g.event_cd = c.code
    WHERE alf_e IS NOT NULL
    AND c.vendor_system = 'EMIS'
    AND event_dt >= '2020-01-01'
    AND event_dt <= CURRENT date
    AND c.is_latest = 1
;

-------------------------------------------------------------------------------------------------
-- Insert Long COVID recordings from WLGP for Vision practices
-------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID
    SELECT g.*,
           c.name AS event_cd_phen,
           c.vendor_system AS event_cd_type,
           c.code AS event_cd_full,
           c.desc AS event_cd_description,
           c.category AS event_cd_category,
           ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY event_dt) AS record_order
    FROM SAILWMC_V.C19_COHORT_WLGP_GP_EVENT_CLEANSED g
    INNER JOIN SAILW1151V.HDR30_PHEN_READ_LONG_COVID19 c
    ON (g.event_cd = c.code OR LEFT(g.event_cd,5) = c.code)
    WHERE alf_e IS NOT NULL
    AND c.vendor_system = 'Vision'
    AND event_dt >= '2020-01-01'
    AND event_dt <= CURRENT date
    AND c.is_latest = 1
;

-------------------------------------------------------------------------------------------------
-- Insert acute COVID recordings from WLGP for EMIS practices
-------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID
    SELECT g.*,
           c.name AS event_cd_phen,
           c.vendor_system AS event_cd_type,
           c.code AS event_cd_full,
           c.desc AS event_cd_description,
           c.category AS event_cd_category,
           ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY event_dt) AS record_order
    FROM SAILWMC_V.C19_COHORT_WLGP_GP_EVENT_CLEANSED g
    INNER JOIN SAILW1151V.HDR30_PHEN_READ_ACUTE_COVID19 c
    ON g.event_cd = c.code
    WHERE alf_e IS NOT NULL
    AND c.vendor_system = 'EMIS'
    AND event_dt >= '2020-01-01'
    AND event_dt <= CURRENT date
    AND c.is_latest = 1
;

-------------------------------------------------------------------------------------------------
-- Insert acute COVID recordings from WLGP for Vision practices
-------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID
    SELECT g.*,
           c.name AS event_cd_phen,
           c.vendor_system AS event_cd_type,
           c.code AS event_cd_full,
           c.desc AS event_cd_description,
           c.category AS event_cd_category,
           ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY event_dt) AS record_order
    FROM SAILWMC_V.C19_COHORT_WLGP_GP_EVENT_CLEANSED g
    INNER JOIN SAILW1151V.HDR30_PHEN_READ_ACUTE_COVID19 c
    ON (g.event_cd = c.code OR LEFT(g.event_cd,5) = c.code)
    WHERE alf_e IS NOT NULL
    AND c.vendor_system = 'Vision'
    AND event_dt >= '2020-01-01'
    AND event_dt <= CURRENT date
    AND c.is_latest = 1
;
-- ***********************************************************************************************
-- Basic checks
-- ***********************************************************************************************
SELECT * FROM SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID;

SELECT count(*), count(DISTINCT alf_e) FROM SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID;

SELECT event_cd_phen, count(DISTINCT alf_e) FROM SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID
WHERE YEAR(event_dt) IN (2020,2021,2022)
GROUP BY event_cd_phen;

SELECT month(event_dt), count(*) FROM SAILWMC_V.C19_COHORT_WLGP_GP_EVENT_CLEANSED
WHERE YEAR(event_dt) = 2023 AND alf_e IS NOT NULL AND event_cd IS NOT NULL
GROUP BY month(event_dt);

SELECT count(DISTINCT alf_e) FROM SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID
WHERE record_order > 10;

SELECT * FROM SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID WHERE record_order > 10;

SELECT max(record_order) FROM SAILW1151V.HDR30_PHEN_WLGP_LONG_COVID;
-------------------------------------------------------------------------------------------------
