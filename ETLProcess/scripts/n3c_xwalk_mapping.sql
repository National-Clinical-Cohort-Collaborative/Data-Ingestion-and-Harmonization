---
--- Author: Stephanie Hong 
--- 
--- PCorNet51 to OMOP531
--- Used to translage valueSet mappings from Native source data tables to OMOP 5.3.1 tables 
--- manifest table and the datacount tables are extracted and sent via the DataPartners
--- load the datacount and manifest table
---

/* populated with data from the DataPartners
CREATE TABLE MANIFEST
(
  MANIFEST_ID NUMBER(18, 0) NOT NULL
, SITE_NAME VARCHAR2(200 BYTE)  -- need to support site’s full name in this table
, SITE_ABBREV_name varchar2(50)
, CONTACT_NAME VARCHAR2(200 BYTE)
, CONTACT_EMAIL VARCHAR2(200 BYTE)
, CDM_NAME VARCHAR2(100 BYTE)
, CDM_VERSION VARCHAR2(20 BYTE) NOT NULL
, N3C_PHENOTYPE_YN VARCHAR2(5 BYTE) NOT NULL
, N3C_PHENOTYPE_VERSION NUMBER(18, 1) NOT NULL -- support decimal change
, RUN_DATE timestamp NOT NULL
, UPDATE_DATE timestamp NOT NULL
, NEXT_SUBMISSION_DATE timestamp NOT NULL
-------used internally
, DATASET_STATUS NUMBER(*, 0)           --Updated by the workflow as the data moves throught the ingestion process 
, DATA_PARTNER_ID NUMBER(*, 0) NOT NULL -- used in generating N3C ids for all domain table
, PROCESS_timestamp timestamp NOT NULL
) ;


-- data count table
CREATE TABLE DATACOUNT
(
  DATACOUNT_ID NUMBER(18, 0) NOT NULL
, DOMAIN_NAME VARCHAR2(100 BYTE) NOT NULL
, ROW_COUNT NUMBER(*, 0) NOT NULL
, DATA_PARTNER_ID NUMBER(*, 0) NOT NULL
, MANIFEST_ID NUMBER(18, 0) NOT NULL
, RUN_DATE DATE NOT NULL
,data_loaded NUMBER(*, 0) NOT NULL
,data_ingested NUMBER(*, 0) NOT NULL 
,data_loaded_delta NUMBER(*, 0) NOT NULL
,data_ingested_delta NUMBER(*, 0) NOT NULL
) ;
****/
/*
--person clean
CREATE TABLE cdmh_staging.N3C_PERSON_CLEAN
(
  RECID NUMBER(18, 0) NOT NULL
, PERSON_ID NUMBER(*, 0) NOT NULL
, CREATE_DATE TIMESTAMP
) ;

-- domain map for n3c ids
create table cdmh_staging.N3CDS_domain_map (
    DOMAIN_MAP_ID	NUMBER(18,0),
    MANIFEST_ID	NUMBER(18,0),
    DATA_PARTNER_ID	NUMBER(38,0),
    DOMAIN_NAME	VARCHAR2(100 BYTE),
    SOURCE_ID	VARCHAR2(100 BYTE),
    N3C_ID	VARCHAR2(200 BYTE),
    CREATE_DATE	TIMESTAMP(6)
) ;
*/

-- xwalk tables, gender, ethnicity, race, encounter/visit types
--
--DROP TABLE IF EXISTS cdmh_staging.gender_xwalk
DROP TABLE cdmh_staging.gender_xwalk ;
CREATE TABLE gender_xwalk (
    CDM_NAME 					VARCHAR(100),
    CDM_TBL           VARCHAR(100),
    src_gender 					VARCHAR(20),
    fhir_cd             varchar(100), 
  	target_concept_id			INTEGER			NOT NULL ,
  	target_concept_name			VARCHAR(255)	NOT NULL ,
  	target_domain_id			VARCHAR(20)		NOT NULL ,
  	target_vocabulary_id		VARCHAR(20)		NOT NULL ,
  	target_concept_class_id		VARCHAR(20)		NOT NULL ,
  	target_standard_concept		VARCHAR(1)		NULL ,
  	target_concept_code			VARCHAR(50)		NOT NULL
);

TRUNCATE TABLE cdmh_staging.gender_xwalk;
--PCORnet CDM
insert into cdmh_staging.gender_xwalk
(cdm_name, cdm_tbl, src_gender, fhir_cd, target_concept_id, target_concept_name , target_domain_id , target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'PCORnet', 'DEMOGRAPHIC', 'M', 'male', 8507, 'MALE', 'Gender', 'Gender','Gender','S', 'M');
insert into cdmh_staging.gender_xwalk
(cdm_name, cdm_tbl, src_gender, fhir_cd, target_concept_id, target_concept_name , target_domain_id , target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'PCORnet', 'DEMOGRAPHIC', 'F', 'female', 8532, 'FEMALE', 'Gender', 'Gender','Gender','S', 'F');
insert into cdmh_staging.gender_xwalk
(cdm_name, cdm_tbl, src_gender, fhir_cd, target_concept_id, target_concept_name , target_domain_id , target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'PCORnet', 'DEMOGRAPHIC', 'A', '', 0, 'Ambiguous', 'Gender', 'Gender','Gender','S', '0');
insert into cdmh_staging.gender_xwalk
(cdm_name, cdm_tbl, src_gender, fhir_cd, target_concept_id, target_concept_name , target_domain_id , target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'PCORnet', 'DEMOGRAPHIC', 'NI', 'unknown', 0, 'No Information', 'Gender', 'Gender','Gender','S', '0');
insert into cdmh_staging.gender_xwalk
(cdm_name, cdm_tbl, src_gender, fhir_cd, target_concept_id, target_concept_name , target_domain_id , target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'PCORnet', 'DEMOGRAPHIC', 'OT', 'other', 0, 'Other', 'Gender', 'Gender','Gender','S', '0');
insert into cdmh_staging.gender_xwalk
(cdm_name, cdm_tbl, src_gender, fhir_cd, target_concept_id, target_concept_name , target_domain_id , target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'PCORnet', 'DEMOGRAPHIC', 'UN', 'unknown', 0, 'Unknown', 'Gender', 'Gender','Gender','S', '0');
insert into cdmh_staging.gender_xwalk
(cdm_name, cdm_tbl, src_gender, fhir_cd, target_concept_id, target_concept_name , target_domain_id , target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'ACT', 'OBS_FCT_DEM', 'DEM|SEX:M', 'male', 8507, 'Male', 'Gender', 'Gender','Gender','S', 'M');
insert into cdmh_staging.gender_xwalk
(cdm_name, cdm_tbl, src_gender, fhir_cd, target_concept_id, target_concept_name , target_domain_id , target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'ACT', 'OBS_FCT_DEM', 'DEM|SEX:F', 'female', 8532, 'FEMALE', 'Gender', 'Gender','Gender','S', 'F');
insert into cdmh_staging.gender_xwalk
(cdm_name, cdm_tbl, src_gender, fhir_cd, target_concept_id, target_concept_name , target_domain_id , target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'ACT', 'OBS_FCT_DEM', 'DEM|SEX:NI', 'unknown', 0, 'No Information', 'Gender', 'Gender','Gender','S', '0');
insert into cdmh_staging.gender_xwalk
(cdm_name, cdm_tbl, src_gender, fhir_cd, target_concept_id, target_concept_name , target_domain_id , target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'ACT', 'OBS_FCT_DEM', 'DEM|SEX:A', '', 0, 'Ambiguous', 'Gender', 'Gender','Gender','S', '0');
insert into cdmh_staging.gender_xwalk
(cdm_name, cdm_tbl, src_gender, fhir_cd, target_concept_id, target_concept_name , target_domain_id , target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'ACT', 'OBS_FCT_DEM', 'DEM|SEX:O', 'other', 0, 'Other', 'Gender', 'Gender','Gender','S', '0');


--
---DROP TABLE IF EXISTS cdmh_staging.ethnicity_xwalk
DROP TABLE cdmh_staging.ethnicity_xwalk ;
CREATE TABLE ethnicity_xwalk (
    CDM_NAME 					VARCHAR(100),
    CDM_TBL           VARCHAR(100),
    src_ethnicity 				VARCHAR(100),
    fhir_cd             varchar(100),
  	target_concept_id			INTEGER			NOT NULL ,
  	target_concept_name			VARCHAR(255)	NOT NULL ,
  	target_domain_id			VARCHAR(20)		NOT NULL ,
  	target_vocabulary_id		VARCHAR(20)		NOT NULL ,
  	target_concept_class_id		VARCHAR(20)		NOT NULL ,
  	target_standard_concept		VARCHAR(1)		NULL ,
  	target_concept_code			VARCHAR(50)		NOT NULL
);

insert into cdmh_staging.ethnicity_xwalk
(cdm_name, cdm_tbl, src_ethnicity, fhir_cd, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'PCORnet', 'DEMOGRAPHIC', 'Y', '2135-2', 38003563, 'Hispanic or Latino', 'Ethnicity', 'Ethnicity','Ethnicity','S', 'Hispanic');
insert into cdmh_staging.ethnicity_xwalk
(cdm_name, cdm_tbl, src_ethnicity, fhir_cd, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'PCORnet', 'DEMOGRAPHIC', 'N', '2186-5', 38003564, 'Not Hispanic or Latino', 'Ethnicity', 'Ethnicity','Ethnicity','S', 'Not Hispanic');
insert into cdmh_staging.ethnicity_xwalk
(cdm_name, cdm_tbl, src_ethnicity, fhir_cd, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'ACT', 'OBS_FCT_DEM', 'DEM|HISP:Y', '2135-2', 38003563, 'Hispanic or Latino', 'Ethnicity', 'Ethnicity','Ethnicity','S', 'Hispanic');
insert into cdmh_staging.ethnicity_xwalk
(cdm_name, cdm_tbl, src_ethnicity, fhir_cd, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code )
values
( 'ACT', 'OBS_FCT_DEM', 'DEM|HISP:N', '2186-5', 38003564, 'Not Hispanic or Latino', 'Ethnicity', 'Ethnicity','Ethnicity','S', 'Not Hispanic');



--DROP TABLE IF EXISTS cdmh_staging.race_xwalk
DROP TABLE cdmh_staging.race_xwalk ;
CREATE TABLE race_xwalk (
    CDM_NAME 					VARCHAR(100),
    CDM_TBL           VARCHAR(100),
    src_race 				VARCHAR(100),
    fhir_cd             varchar(100),
  	target_concept_id			INTEGER			NOT NULL ,
  	target_concept_name			VARCHAR(255)	NOT NULL ,
  	target_domain_id			VARCHAR(20)		NOT NULL ,
  	target_vocabulary_id		VARCHAR(20)		NOT NULL ,
  	target_concept_class_id		VARCHAR(20)		NOT NULL ,
  	target_standard_concept		VARCHAR(1)		NULL ,
  	target_concept_code			VARCHAR(50)		NOT NULL
);
--PCORNet race values: 
--8657 DEMOGRAPHIC  RACE  01  01=American  Indian  or  Alaska  Native
--8515 DEMOGRAPHIC  RACE  02  02=Asian
--8516 DEMOGRAPHIC  RACE  03  03=Black  or  African  American
--8557 DEMOGRAPHIC  RACE  04  04=Native  Hawaiian  or  Other  Pacific  Islander
--8527 DEMOGRAPHIC  RACE  05  05=White
--ACT values:
--DEM|RACE:NA American Indian or Alaska Native
--DEM|RACE:AS Asian
--DEM|RACE:B  Black or African American
--DEM|RACE:M  Multiple race
--DEM|RACE:H  Native Hawaiian or Other Pacific Islander
--DEM|RACE:NI No information
--DEM|RACE:W  White

insert into cdmh_staging.race_xwalk 
(cdm_name, cdm_tbl, src_race, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code  )
values ( 'PCORnet', 'DEMOGRAPHIC', '01', '1002-5', 8657, 'American  Indian  or  Alaska  Native', 'Race', 'Race','Race','S', '1');

insert into cdmh_staging.race_xwalk 
(cdm_name, cdm_tbl, src_race, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code  )
values ( 'PCORnet', 'DEMOGRAPHIC', '02', '2028-9', 8515, 'Asian', 'Race', 'Race','Race','S', '2') ;

Insert into cdmh_staging.race_xwalk 
(cdm_name, cdm_tbl, src_race, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code  )
values ( 'PCORnet', 'DEMOGRAPHIC', '03', '2054-5', 8516, 'Black  or  African  American', 'Race', 'Race','Race','S', '3') ;

Insert into cdmh_staging.race_xwalk 
(cdm_name, cdm_tbl, src_race, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code  )
values ( 'PCORnet', 'DEMOGRAPHIC', '04', '2076-8', 8557, 'Native  Hawaiian  or  Other  Pacific  Islander', 'Race', 'Race','Race','S', '4');

Insert into cdmh_staging.race_xwalk 
(cdm_name, cdm_tbl, src_race, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code  )
values ( 'PCORnet', 'DEMOGRAPHIC', '05', '2106-3', 8527, 'White', 'Race', 'Race','Race','S', '5') ;

--ACT
nsert into cdmh_staging.race_xwalk 
(cdm_name, cdm_tbl, src_race, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code  )
values ( 'ACT', 'OBS_FCT_DEM', 'DEM|RACE:NA', '1002-5', 8657, 'American  Indian  or  Alaska  Native', 'Race', 'Race','Race','S', '1');

insert into cdmh_staging.race_xwalk 
(cdm_name, cdm_tbl, src_race, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code  )
values ( 'ACT', 'OBS_FCT_DEM', 'DEM|RACE:AS', '2028-9', 8515, 'Asian', 'Race', 'Race','Race','S', '2') ;

Insert into cdmh_staging.race_xwalk 
(cdm_name, cdm_tbl, src_race, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code  )
values ( 'ACT', 'OBS_FCT_DEM', 'DEM|RACE:B', '2054-5', 8516, 'Black  or  African  American', 'Race', 'Race','Race','S', '3') ;

Insert into cdmh_staging.race_xwalk 
(cdm_name, cdm_tbl, src_race, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code  )
values ( 'ACT', 'OBS_FCT_DEM', 'DEM|RACE:H', '2076-8', 8557, 'Native  Hawaiian  or  Other  Pacific  Islander', 'Race', 'Race','Race','S', '4');

Insert into cdmh_staging.race_xwalk 
(cdm_name, cdm_tbl, src_race, target_concept_id, target_concept_name, target_domain_id, target_vocabulary_id, target_concept_class_id, target_standard_concept, target_concept_code  )
values ( 'ACT', 'OBS_FCT_DEM', 'DEM|RACE:W', '2106-3', 8527, 'White', 'Race', 'Race','Race', 'S', '5') ;



--drop table if exists
DROP TABLE cdmh_staging.visit_xwalk ;
CREATE TABLE visit_xwalk (
    CDM_NAME 					VARCHAR(100),
    CDM_TBL           VARCHAR(100),
    src_visit_type 				VARCHAR(100),
    fhir_cd             varchar(100),
  	target_concept_id			INTEGER			NOT NULL ,
  	target_concept_name			VARCHAR(255)	NOT NULL ,
  	target_domain_id			VARCHAR(20)		NOT NULL ,
  	target_vocabulary_id		VARCHAR(20)		NOT NULL ,
  	target_concept_class_id		VARCHAR(20)		NOT NULL ,
  	target_standard_concept		VARCHAR(1)		NULL ,
  	target_concept_code			VARCHAR(50)		NOT NULL
);

/*
visit:
'9201','Inpatient Visit','IP'
'9202','Outpatient Visit','OP'
'9203','Emergency Room Visit','ER'
'42898160','Long Term Care Visit','LTCP'
visit type:
'44818517','Visit derived from encounter on claim','OMOP generated'
'44818518','Visit derived from EHR record','OMOP generated'
'44818519','Clinical Study visit','OMOP generated'
*/