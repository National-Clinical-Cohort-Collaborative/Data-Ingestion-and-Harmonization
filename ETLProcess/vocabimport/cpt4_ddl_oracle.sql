
/**************************************************
create table for concept cpt4
Used for cpt4 concepts in OMOP V.5.3.1.
Project: DI&H . 
EditDate: 5/15/20 
Author: Stephanie Hong
***************************************************/


create table concept_cpt4
(
    CONCEPT_ID	NUMBER(38,0),
    CONCEPT_NAME	VARCHAR2(255 BYTE),
    DOMAIN_ID	VARCHAR2(20 BYTE),
    VOCABULARY_ID	VARCHAR2(20 BYTE),
    CONCEPT_CLASS_ID	VARCHAR2(20 BYTE),
    STANDARD_CONCEPT	VARCHAR2(1 BYTE),
    CONCEPT_CODE	VARCHAR2(50 BYTE),
    VALID_START_DATE	DATE,
    VALID_END_DATE	DATE,
    INVALID_REASON	VARCHAR2(1 BYTE)

) ;