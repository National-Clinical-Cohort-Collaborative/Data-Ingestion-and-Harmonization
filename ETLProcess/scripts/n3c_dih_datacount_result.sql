
/********************************************************************
project : N3C DI&H
Date: 5/16/2020
Author: Stephanie Hong
Description : Each data payload from the contributing site will contain manifest table with data sent


Compute difference btw data sent and data loaded in order to compare
*********************************************************************/

--DATA_COUNTS TABLE
--Data count from the DATA_COUNTS.csv is checked against the data loaded to each table:
-- Count data from Native CDM schema
--
--

SELECT X.table_name, X.ROW_COUNT_LOADED, dc.ROW_COUNT, dc.row_count - x.ROW_COUNT_LOADED as diff  
from
(select 
       'PERSON' as TABLE_NAME, 
       (select count(*) from NATIVE_OMOP522_CDM.PERSON ) as ROW_COUNT_LOADED
    from DUAL ---( select ROW_COUNT_LOADED as ROW_COUNT_LOADED_src from NATIVE_OMOP522_CDM.DATACOUNT ) x ;
    UNION
    select 
   'DEATH' as TABLE_NAME, 
   (select count(*) from NATIVE_OMOP522_CDM.DEATH ) as ROW_COUNT_LOADED
from DUAL
UNION
select 
   'SPECIMEN' as TABLE_NAME, 
   (select count(*) from NATIVE_OMOP522_CDM.SPECIMEN ) as ROW_COUNT_LOADED
from DUAL
UNION
select 
   'OBSERVATION_PERIOD' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.OBSERVATION_PERIOD ) as ROW_COUNT_LOADED
from DUAL
UNION
select 
   'VISIT_OCCURRENCE' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.VISIT_OCCURRENCE) as ROW_COUNT_LOADED
from DUAL

UNION

select 
   'CONDITION_OCCURRENCE' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.CONDITION_OCCURRENCE) as ROW_COUNT_LOADED
from DUAL

UNION

select 
   'DRUG_EXPOSURE' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.DRUG_EXPOSURE ) as ROW_COUNT_LOADED
from DUAL

UNION

select 
   'PROCEDURE_OCCURRENCE' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.PROCEDURE_OCCURRENCE ) as ROW_COUNT_LOADED
from DUAL

UNION
   
select 
   'MEASUREMENT' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.MEASUREMENT) as ROW_COUNT_LOADED
from DUAL

UNION

select 
   'OBSERVATION' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.OBSERVATION ) as ROW_COUNT_LOADED
from DUAL

UNION

----
--NOTE, OMOP does not have PERSON_ID for Location, Care Site and Provider tables
--Location_id, care_site_id, and provider_id is in the Person table. 

select 
   'LOCATION' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.LOCATION) as ROW_COUNT_LOADED
from DUAL

UNION

select 
   'CARE_SITE' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.CARE_SITE) as ROW_COUNT_LOADED
from DUAL

UNION
 
 select 
   'PROVIDER' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.PROVIDER) as ROW_COUNT_LOADED
from DUAL

UNION

select 
   'DRUG_ERA' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.DRUG_ERA) as ROW_COUNT_LOADED
from DUAL

UNION

select 
   'DOSE_ERA' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.DOSE_ERA) as ROW_COUNT_LOADED
from DUAL

UNION

select 
   'CONDITION_ERA' as TABLE_NAME,
   (select count(*) from NATIVE_OMOP522_CDM.CONDITION_ERA ) as ROW_COUNT_LOADED
from DUAL


) x join CDMH_STAGING.DATACOUNT dc
on UPPER( dc.domain_name) = UPPER( x.TABLE_NAME)  
;