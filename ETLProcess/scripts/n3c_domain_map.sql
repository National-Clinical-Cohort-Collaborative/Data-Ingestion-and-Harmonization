

/**
project : N3C DI&H
Date: 5/16/2020
Author: Stephanie Hong
Description : n3c domain id generation to prevent merge of source dataSet colliding with each other at the contributing target
N3C dataStore db

n3c ids are generated for the following tables
person, observation_period, specimen, 
visit_occurrence, observation, condition_occurrence, procedure_occurrence, drug_exposure, device_expore, measurement, note

domain_map table spec:
    DOMAIN_MAP_ID   NUMBER(18,0), --record id 
    MANIFEST_ID NUMBER(18,0), --id from the manifest table
    DATA_PARTNER_ID NUMBER(38,0), --data partner id assigned to each data site --from the manifest table 
    DOMAIN_NAME VARCHAR2(100 BYTE), --src domain /table name 
    SOURCE_ID   VARCHAR2(100 BYTE), -- person_id, oberservation_id
    N3C_ID  VARCHAR2(200 BYTE),                                     
    CREATE_DATE TIMESTAMP(6)
) ;


**/

create table domain_map (
    DOMAIN_MAP_ID	NUMBER(18,0), --record id 
    MANIFEST_ID	NUMBER(18,0), --id from the manifest table
    DATA_PARTNER_ID	NUMBER(38,0), --data partner id assigned to each data site --from the manifest table 
    DOMAIN_NAME	VARCHAR2(100 BYTE), --src domain /table name 
    SOURCE_ID	VARCHAR2(100 BYTE), -- person_id, oberservation_id
    N3C_ID	VARCHAR2(200 BYTE),                                     
    CREATE_DATE	TIMESTAMP(6)
) ;

---N3C_DATA_LOAD_DATAPARTNERID
---N3C_DATA_ING_
CREATE SEQUENCE domain_map_id_seq
    START WITH 1
    INCREMENT BY 1
    NOMAXVALUE
    NOCYCLE
    CACHE 20;