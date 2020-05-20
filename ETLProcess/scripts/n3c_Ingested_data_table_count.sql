/********************************************************************
project : N3C DI&H
Date: 5/16/2020
Author: Stephanie Hong
Description : 
Each data payload from the contributing site will contain manifest table with data sent
Count the ingested data:


*********************************************************************/

SELECT table_name, row_count from
    (select 
       'PERSON' as TABLE_NAME, 
       (select count(*) from N3C_OMOP531_INSTANCE.PERSON ) as ROW_COUNT
    from DUAL ---( select row_count as row_count_src from N3C_OMOP531_INSTANCE.DATACOUNT ) x ;
    UNION
    select 
   'DEATH' as TABLE_NAME, 
   (select count(*) from N3C_OMOP531_INSTANCE.DEATH ) as ROW_COUNT
from DUAL
UNION
select 
   'SPECIMEN' as TABLE_NAME, 
   (select count(*) from N3C_OMOP531_INSTANCE.SPECIMEN ) as ROW_COUNT
from DUAL
UNION
select 
   'OBSERVATION_PERIOD' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.OBSERVATION_PERIOD ) as ROW_COUNT
from DUAL
UNION
select 
   'VISIT_OCCURRENCE' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.VISIT_OCCURRENCE) as ROW_COUNT
from DUAL

UNION

select 
   'CONDITION_OCCURRENCE' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.CONDITION_OCCURRENCE) as ROW_COUNT
from DUAL

UNION

select 
   'DRUG_EXPOSURE' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.DRUG_EXPOSURE ) as ROW_COUNT
from DUAL

UNION

select 
   'PROCEDURE_OCCURRENCE' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.PROCEDURE_OCCURRENCE ) as ROW_COUNT
from DUAL

UNION
   
select 
   'MEASUREMENT' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.MEASUREMENT) as ROW_COUNT
from DUAL

UNION

select 
   'OBSERVATION' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.OBSERVATION ) as ROW_COUNT
from DUAL

UNION

--OMOP does not have PERSON_ID for Location, Care Site and Provider tables so we need to determine the applicability of this check
--We could re-engineer the cohort table to include the JOIN variables
select 
   'LOCATION' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.LOCATION) as ROW_COUNT
from DUAL

UNION

select 
   'CARE_SITE' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.CARE_SITE) as ROW_COUNT
from DUAL

UNION
 
 select 
   'PROVIDER' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.PROVIDER) as ROW_COUNT
from DUAL

UNION

select 
   'DRUG_ERA' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.DRUG_ERA) as ROW_COUNT
from DUAL

UNION

select 
   'DOSE_ERA' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.DOSE_ERA) as ROW_COUNT
from DUAL

UNION

select 
   'CONDITION_ERA' as TABLE_NAME,
   (select count(*) from N3C_OMOP531_INSTANCE.CONDITION_ERA ) as ROW_COUNT
from DUAL) ; 
;

) x on UPPER(ds.table_name) = UPPER( x.TABLE_NAME)  