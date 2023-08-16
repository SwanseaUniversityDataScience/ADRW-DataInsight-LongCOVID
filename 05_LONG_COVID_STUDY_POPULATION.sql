--************************************************************************************************
-- Script:        05_LONG_COVID_STUDY_POPULATION.sql
-- SAIL project:  1151 - Wales Multi-morbidity cohort - Census Data
-- HDR project:   HDR30 - Clinical coding of Long COVID-19 in Wales

-- About:         Create a cohort of individuals with a Long COVID record
-- Author:        Hoda Abbasizanjani
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Study population, coverage untill end of 2022
-- ***********************************************************************************************
CREATE TABLE SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION_2022 (
    alf_e                          bigint,
    wob                            date,
    dod                            date,
    gndr_cd                        char(1),
    age                            int,
    ethnicity                      char(5),
    lsoa2011                       char(10), -- At 1st January 20202, using C20 cohort
    wimd2019                       char(10), -- WIMD2019 (based on LSOA2011)
    care_home                      char(1),
    age_Jan_2020                   int,
    c20_cohort_start_dt            date,
    c20_cohort_end_dt              date,
    c20_migration_date             date,
    wlgp                           smallint,
    pedw                           smallint,
    opdw                           smallint,
    confirmed_covid                smallint,
    hospitalised                   smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION_2022;
--TRUNCATE TABLE SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION_2022 IMMEDIATE;

------------------------------------------------------------------------------------------------
-- Insert all individuals with a record of long COVID
------------------------------------------------------------------------------------------------
INSERT INTO SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION_2022 (alf_e,wob,dod,gndr_cd,lsoa2011,wimd2019,care_home,age_Jan_2020,
    c20_cohort_start_dt,c20_cohort_end_dt,c20_migration_date,wlgp,pedw,opdw)
    WITH wlgp AS (SELECT DISTINCT alf_e, 1 AS flag
                  FROM SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS
                  WHERE long_covid_data_source = 'WLGP'
                  AND year(long_covid_date) IN (2020,2021,2022)
                  ),
         pedw AS (SELECT DISTINCT alf_e, 1 AS flag
                  FROM SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS
                  WHERE long_covid_data_source = 'PEDW'
                  AND year(long_covid_date) IN (2020,2021,2022)
                  ),
         opdw AS (SELECT DISTINCT alf_e, 1 AS flag
                  FROM SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS
                  WHERE long_covid_data_source = 'OPDW'
                  AND year(long_covid_date) IN (2020,2021,2022)
                  )
    SELECT c.alf_e,
           c.wob,
           dod_jl,
           gndr_cd,
           lsoa2011_inception,
           wimd2019_quintile_inception,
           CASE WHEN carehome_ralf_inception IS NOT NULL THEN 1
                ELSE NULL
           END AS C20_care_home,
           der_age_,
           cohort_start_date,
           cohort_end_date,
           migration_date,
           wlgp.flag,
           pedw.flag,
           opdw.flag
     FROM SAILWMC_V.C19_COHORT20 c
     LEFT JOIN wlgp
     ON c.alf_e = wlgp.alf_e
     LEFT JOIN pedw
     ON c.alf_e = pedw.alf_e
     LEFT JOIN opdw
     ON c.alf_e = opdw.alf_e
     WHERE (wlgp.flag IS NOT NULL OR pedw.flag IS NOT NULL OR opdw.flag IS NOT NULL)
;

-------------------------------------------------------------------------------------------------
-- Add age (at 2022-12-31)
-------------------------------------------------------------------------------------------------
UPDATE SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION_2022
SET age = TIMESTAMPDIFF(256,TIMESTAMP('2022-12-31') - TIMESTAMP(wob));

-------------------------------------------------------------------------------------------------
-- Add ethnicity
-------------------------------------------------------------------------------------------------
UPDATE SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION_2022 tgt
SET ethnicity = ec_ons_desc
FROM SAILW1151V.ETHN_EMG2_EC_C20 src
WHERE tgt.alf_e = src.alf_e;

-------------------------------------------------------------------------------------------------
-- Add history of COVID-19 infection
-------------------------------------------------------------------------------------------------
UPDATE SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION_2022
SET confirmed_covid = 1
WHERE alf_e IN (SELECT alf_e
                FROM (SELECT a.alf_e, first_long_covid_date, covid19_confirmed_date,
                             CASE WHEN first_long_covid_date > covid19_confirmed_date THEN 1 ELSE 0 END AS prev_infc,
                             CASE WHEN first_long_covid_date - 14 DAYS > covid19_confirmed_date THEN 1 ELSE 0 END AS prev_infc_14
                      FROM (SELECT alf_e,
                                   min(long_covid_date) AS first_long_covid_date -- First diagnosis date
                            FROM SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS
                            WHERE year(long_covid_date) IN (2020,2021,2022)
                            GROUP BY alf_e
                            ) a
                      INNER JOIN SAILW1151V.HDR30_ALL_CONFIRMED_COVID v
                      ON a.alf_e = v.alf_e
                      WHERE YEAR(covid19_confirmed_date) IN (2020,2021,2022)
                      )
               --WHERE prev_infc_14 = 1
               WHERE prev_infc = 1
                )
;

-------------------------------------------------------------------------------------------------
-- Add history of hospitalisation within 28 days of a previous COVID-19 infection
-------------------------------------------------------------------------------------------------
UPDATE SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION_2022
SET hospitalised = 1
WHERE alf_e IN (SELECT alf_e
                FROM (SELECT a.alf_e, first_long_covid_date, covid19_confirmed_date,
                             CASE WHEN first_long_covid_date > covid19_confirmed_date THEN 1 ELSE 0 END AS hospitalised
                      FROM (SELECT alf_e,
                                   min(long_covid_date) AS first_long_covid_date -- First diagnosis date
                            FROM SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS
                            WHERE year(long_covid_date) IN (2020,2021,2022)
                            GROUP BY alf_e
                            ) a
                      INNER JOIN (SELECT c.*, s.admis_dt, TIMESTAMPDIFF(16,TIMESTAMP(admis_dt) - TIMESTAMP(covid19_confirmed_date))
                                  FROM SAILW1151V.HDR30_ALL_CONFIRMED_COVID c
                                  INNER JOIN SAILWMC_V.C19_COHORT_PEDW_SPELL s
                                  ON c.alf_e = s.alf_e
                                  WHERE YEAR(admis_dt) IN (2020,2021,2022)
                                  AND TIMESTAMPDIFF(16,TIMESTAMP(admis_dt) - TIMESTAMP(covid19_confirmed_date)) <= 28
                                  AND admis_dt > covid19_confirmed_date -- To include only the admissions recorded after first COVID infection
                                 ) v
                      ON a.alf_e = v.alf_e
                      WHERE covid19_confirmed_date IS NOT NULL
                      )
               WHERE hospitalised = 1
                )
;

-- ***********************************************************************************************
-- Basic checks
-- ***********************************************************************************************
SELECT count(DISTINCT ALF_E)
FROM (SELECT c.*, s.admis_dt, TIMESTAMPDIFF(16,TIMESTAMP(admis_dt) - TIMESTAMP(covid19_confirmed_date))
      FROM SAILW1151V.HDR30_ALL_CONFIRMED_COVID c
      INNER JOIN SAILWMC_V.C19_COHORT_PEDW_SPELL s
      ON c.alf_e = s.alf_e
      WHERE YEAR(admis_dt) IN (2020,2021,2022)
      AND TIMESTAMPDIFF(16,TIMESTAMP(admis_dt) - TIMESTAMP(covid19_confirmed_date)) <= 28
      AND admis_dt > covid19_confirmed_date
      )
;

SELECT count(DISTINCT ALF_E)
FROM SAILW1151V.HDR30_ALL_CONFIRMED_COVID
WHERE YEAR(covid19_confirmed_date) IN (2020,2021,2022);

SELECT count(DISTINCT ALF_E) FROM SAILW1151V.HDR30_LONG_COVID_ALL_RECORDS
WHERE long_covid_data_source = 'PEDW'
and YEAR(long_covid_date) IN (2020,2021,2022);

SELECT * FROM SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION_2022
WHERE care_home IS NOT NULL;

SELECT count(DISTINCT alf_e) FROM SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION_2022
WHERE confirmed_covid = 1;

SELECT gndr_cd, count(*) FROM SAILW1151V.HDR30_LONG_COVID_STUDY_POPULATION_2022
GROUP BY gndr_cd;
----------------------------------------------------------------------------------