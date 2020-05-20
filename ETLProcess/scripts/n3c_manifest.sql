/**
project : N3C DI&H
Date: 5/16/2020
Author: Stephanie Hong
Description : Each data payload from the contributing site will contain manifest table 

--site’s MANIFEST TABLE example: 
    'site's full name' as SITE_FULL_NAME
       'OHDSI' as SITE_ABBREV,
       'Jane Doe' as CONTACT_NAME,
       'jane_doe@OHDSI.edu' as CONTACT_EMAIL,
       'OMOP' as CDM_NAME,
       '5.2.2' as CDM_VERSION,
       'N' as N3C_PHENOTYPE_YN,
       '1' as N3C_PHENOTYPE_VERSION,
       TO_CHAR(sysdate, 'YYYY-MM-DD HH24:MI:SS') as RUN_DATE,                 -- data run date
       TO_CHAR(sysdate +2, 'YYYY-MM-DD HH24:MI:SS') as UPDATE_DATE,           --change integer based on your site's data latency
       TO_CHAR(sysdate +5, 'YYYY-MM-DD HH24:MI:SS') as NEXT_SUBMISSION_DATE   --change integer based on your site's next schedule submission 
    from dual;

--DataSet_status codes:
1 DataSet Received
2 Staging DataSet in Process
3 Staging DataSet Complete
4 Generating Person Clean
5 Generated Person Clean
6 Generating Domain Map Id
7 Generated Domain Map Id
8 Load to Instance db in Progress
9 Load to Instance db Complete
100 Queued -- The data payload are queued. Currently, only one payload is processed start to finish, before taking on the next dataSet.  

**/

CREATE TABLE MANIFEST 
(
  MANIFEST_ID NUMBER(18, 0) NOT NULL
, SITE_NAME VARCHAR2(200 BYTE) 
, SITE_ABBREV_name varchar2(50)
, CONTACT_NAME VARCHAR2(200 BYTE) 
, CONTACT_EMAIL VARCHAR2(200 BYTE) 
, CDM_NAME VARCHAR2(100 BYTE) 
, CDM_VERSION VARCHAR2(20 BYTE) NOT NULL 
, N3C_PHENOTYPE_YN VARCHAR2(5 BYTE) NOT NULL 
, N3C_PHENOTYPE_VERSION NUMBER(18, 1) NOT NULL  <----- need to have decimal
, RUN_DATE timestamp NOT NULL 
, UPDATE_DATE timestamp NOT NULL 
, NEXT_SUBMISSION_DATE timestamp NOT NULL 
-------used internally
, DATASET_STATUS NUMBER(*, 0) 
, DATA_PARTNER_ID NUMBER(*, 0) NOT NULL 
, PROCESS_timestamp timestamp NOT NULL 
, VOCABULARY_VERSION VARCHAR2(20 BYTE)
) ;

CREATE SEQUENCE manifest_id_seq
    START WITH 1
    INCREMENT BY 1
    NOMAXVALUE
    NOCYCLE
    CACHE 20;