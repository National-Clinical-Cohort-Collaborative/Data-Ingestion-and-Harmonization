/**
project : N3C DI&H
Date: 5/16/2020
Author: Stephanie Hong
Description : n3c domain id generation to prevent merge of source dataSet colliding with each other at the contributing target
N3C dataStore db

n3c ids are generated for the following tables
person, observation_period, specimen, 
visit_occurrence, observation, condition_occurrence, procedure_occurrence, drug_exposure, device_expore, measurement, note
location, care_site, and provider

n3c_domain_map table spec:
    DOMAIN_MAP_ID   NUMBER(18,0), --record id 
    MANIFEST_ID NUMBER(18,0), --id from the manifest table
    DATA_PARTNER_ID NUMBER(38,0), --data partner id assigned to each data site --from the manifest table 
    DOMAIN_NAME VARCHAR2(100 BYTE), --src domain /table name 
    SOURCE_ID   VARCHAR2(100 BYTE), -- person_id, oberservation_id
    N3C_ID  VARCHAR2(200 BYTE),     --generated n3c id                                
    CREATE_DATE TIMESTAMP(6) -- create date


**/



truncate TABLE N3CDS_domain_map ;
--PERSON
insert into N3CDS_domain_map
    select 
    domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'PERSON' as DOMAIN_NAME, 
    p.person_id as source_id, 
    concat( t1.data_partner_id, p.person_id ) as N3C_ID, sysdate as create_date 
    from CDMH_STAGING.manifest t1 cross join CDMH_STAGING.st_omop522_person_clean p 
    ; --7,065
    --use person_clean 
   -- insert into N3CDS_domain_map
    --select 
    --domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'PERSON' as DOMAIN_NAME, 
    --p.person_id as source_id, 
    --concat( t1.data_partner_id, p.person_id ) as N3C_ID, sysdate as create_date 
    --from CDMH_STAGING.manifest t1 cross join NATIVE_OMOP522_CDM.Person p ;
    ---;--7065 inserted for person

--OBSERVATION_PERIOD
insert into N3CDS_domain_map
        select 
        domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'OBSERVATION_PERIOD' as DOMAIN_NAME, 
        x.observation_period_id as source_id, 
        concat( t1.data_partner_id, x.observation_period_id ) as N3C_ID, sysdate as create_date 
        from CDMH_STAGING.manifest t1 
        cross join 
        --NATIVE_OMOP522_CDM.OBSERVATION_PERIOD op ; --0 inserted for observation_period --uwstl did not send any observation_period
        ( select obp.observation_period_id as observation_period_id, obp.person_id as person_id
            from CDMH_STAGING.st_omop522_person_clean pc JOIN NATIVE_OMOP522_CDM.OBSERVATION_PERIOD obp ON obp.PERSON_ID = pc.PERSON_ID ) x 
        ;---126,671 rows 
        
--VISIT_OCCURRENCE
 insert into N3CDS_domain_map
    select 
    domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'VISIT_OCCURRENCE' as DOMAIN_NAME, 
    x.VISIT_OCCURRENCE_ID  as source_id, 
    concat( t1.data_partner_id, x.VISIT_OCCURRENCE_ID ) as N3C_ID, sysdate as create_date 
    from CDMH_STAGING.manifest t1 
    cross join --NATIVE_OMOP522_CDM.visit_occurrence vo 
    ( select vo.VISIT_OCCURRENCE_ID as VISIT_OCCURRENCE_ID, pc.person_id as person_id
            from CDMH_STAGING.st_omop522_person_clean pc JOIN NATIVE_OMOP522_CDM.VISIT_OCCURRENCE vo ON vo.PERSON_ID = pc.PERSON_ID ) x 
    ; --29,406
    
--PROCEDURE_OCCURRENCE
insert into N3CDS_domain_map
    select 
    domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'PROCEDURE_OCCURRENCE' as DOMAIN_NAME, --domainname
    x.procedure_occurrence_id  as source_id, -- source id
    concat( t1.data_partner_id, x.procedure_occurrence_id ) as N3C_ID, --N3C_ID
    sysdate as create_date --
    from CDMH_STAGING.manifest t1 
    cross join --NATIVE_OMOP522_CDM.procedure_occurrence po ; ---2,280
    ( select po.PROCEDURE_OCCURRENCE_ID as PROCEDURE_OCCURRENCE_ID, pc.person_id as person_id
            from CDMH_STAGING.st_omop522_person_clean pc JOIN NATIVE_OMOP522_CDM.PROCEDURE_OCCURRENCE po ON po.PERSON_ID = pc.PERSON_ID ) x 
    ;--2280
    
--DRUG_EXPOSURE
insert into N3CDS_domain_map
    select 
    domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'DRUG_EXPOSURE' as DOMAIN_NAME, --domainname
    x.drug_exposure_id  as source_id, -- source id
    concat( t1.data_partner_id, x.DRUG_EXPOSURE_ID ) as N3C_ID, --N3C_ID
    sysdate as create_date --
    from CDMH_STAGING.manifest t1 
    cross join --NATIVE_OMOP522_CDM.DRUG_EXPOSURE de ; ---2,280
    ( select dr.DRUG_EXPOSURE_ID as DRUG_EXPOSURE_ID, pc.person_id as person_id
            from CDMH_STAGING.st_omop522_person_clean pc JOIN NATIVE_OMOP522_CDM.DRUG_EXPOSURE dr ON dr.PERSON_ID = pc.PERSON_ID ) x 
    ;--416,527
    
---DEVICE_EXPOSURE
insert into N3CDS_domain_map
    select 
    domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'DEVICE_EXPOSURE' as DOMAIN_NAME, --domainname
    x.device_exposure_id  as source_id, -- source id
    concat( t1.data_partner_id, x.DEVICE_EXPOSURE_ID ) as N3C_ID, --N3C_ID
    sysdate as create_date --
    from CDMH_STAGING.manifest t1 
    cross join --NATIVE_OMOP522_CDM.device_EXPOSURE de ; ---2,280
    ( select de.DEVICE_EXPOSURE_ID as DEVICE_EXPOSURE_ID, pc.person_id as person_id
            from CDMH_STAGING.st_omop522_person_clean pc JOIN NATIVE_OMOP522_CDM.DEVICE_EXPOSURE de ON de.PERSON_ID = pc.PERSON_ID ) x 
    ;--0
    
--CONDITION_OCCURRENCE    
insert into N3CDS_domain_map
    select 
    domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'CONDITION_OCCURRENCE' as DOMAIN_NAME, --domainname
    x.CONDITION_OCCURRENCE_ID  as source_id, -- source id
    concat( t1.data_partner_id, x.CONDITION_OCCURRENCE_ID ) as N3C_ID, --N3C_ID
    sysdate as create_date --
    from CDMH_STAGING.manifest t1 
    cross join --NATIVE_OMOP522_CDM.CONDITION_OCCURRENCE_ID co ; ---2,280
    ( select co.CONDITION_OCCURRENCE_ID as CONDITION_OCCURRENCE_ID, pc.person_id as person_id
            from CDMH_STAGING.st_omop522_person_clean pc JOIN NATIVE_OMOP522_CDM.CONDITION_OCCURRENCE co ON co.PERSON_ID = pc.PERSON_ID ) x 
    ;--294,175
    
--MEASUREMENT
insert into N3CDS_domain_map
        select 
        domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'MEASUREMENT' as DOMAIN_NAME, 
        x.MEASUREMENT_ID as source_id, 
        concat( t1.data_partner_id, x.MEASUREMENT_ID ) as N3C_ID, sysdate as create_date 
        from CDMH_STAGING.manifest t1 
        cross join --NATIVE_OMOP522_CDM.MEASUREMENT me  
        ( select MEASUREMENT_ID as MEASUREMENT_id, pc.person_id as person_id
            from CDMH_STAGING.st_omop522_person_clean pc JOIN NATIVE_OMOP522_CDM.MEASUREMENT me ON me.PERSON_ID = pc.PERSON_ID ) x 
    ;--4,447,332 rows inserted.
            
-- SPECIMEN
insert into N3CDS_domain_map
        select 
        domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'SPECIMEN' as DOMAIN_NAME, 
        x.specimen_id as source_id, 
        concat( t1.data_partner_id, x.specimen_id ) as N3C_ID, sysdate as create_date 
        from CDMH_STAGING.manifest t1 
        cross join --NATIVE_OMOP522_CDM.SPECIMEN sp  
        ( select sp.SPECIMEN_ID as SPECIMEN_ID, pc.person_id as person_id
            from CDMH_STAGING.st_omop522_person_clean pc JOIN NATIVE_OMOP522_CDM.SPECIMEN sp ON sp.PERSON_ID = pc.PERSON_ID ) x 
    ; --0 rows inserted.
 
 --DEATH 
--select *  from NATIVE_OMOP522_CDM.death ;
insert into N3CDS_domain_map
        select 
        domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'DEATH' as DOMAIN_NAME, 
        x.person_id as source_id, 
        concat( t1.data_partner_id, x.person_id ) as N3C_ID, sysdate as create_date 
        from CDMH_STAGING.manifest t1 
        cross join 
        ( select distinct person_id from NATIVE_OMOP522_CDM.death  ) x
        ;--908

 -- NOTE
insert into N3CDS_domain_map
        select 
        domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'NOTE' as DOMAIN_NAME, 
        x.note_id as source_id, 
        concat( t1.data_partner_id, x.note_id ) as N3C_ID, sysdate as create_date 
        from CDMH_STAGING.manifest t1 
        cross join --NATIVE_OMOP522_CDM.NOTE nt  
        ( select nt.NOTE_ID as NOTE_ID, pc.person_id as person_id
            from CDMH_STAGING.st_omop522_person_clean pc JOIN NATIVE_OMOP522_CDM.NOTE nt ON nt.PERSON_ID = pc.PERSON_ID ) x 
    ; --0 rows inserted.
 
 --Observation
insert into N3CDS_domain_map
        select 
        domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'OBSERVATION' as DOMAIN_NAME, 
        x.OBSERVATION_ID as source_id, 
        concat( t1.data_partner_id, x.OBSERVATION_ID ) as N3C_ID, sysdate as create_date 
        from CDMH_STAGING.manifest t1 
        cross join --NATIVE_OMOP522_CDM.OBSERVATION ob  
        ( select ob.OBSERVATION_ID as OBSERVATION_ID, pc.person_id as person_id
            from CDMH_STAGING.st_omop522_person_clean pc JOIN NATIVE_OMOP522_CDM.OBSERVATION ob ON ob.PERSON_ID = pc.PERSON_ID ) x 
    ; --161,016 rows inserted.
 
 
--LOCATION
insert into N3CDS_domain_map
    select 
    domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'LOCATION' as DOMAIN_NAME, 
    lo.location_id  as source_id, --
    concat( t1.data_partner_id, lo.location_id ) as N3C_ID, sysdate as create_date --N3C_ID
    from CDMH_STAGING.manifest t1 
    cross join 
    NATIVE_OMOP522_CDM.LOCATION lo ; ---6,827 rows inserted
    
-- CARE_SITE
insert into N3CDS_domain_map
        select
        domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'CARE_SITE' as DOMAIN_NAME, 
        cs.care_site_id as source_id, 
        concat( t1.data_partner_id, cs.care_site_id ) as N3C_ID, sysdate as create_date 
        from CDMH_STAGING.manifest t1 
        cross join 
        NATIVE_OMOP522_CDM.CARE_SITE cs  ;--20 rows inserted

--PROVIDER
insert into N3CDS_domain_map
        select
        domain_map_id_seq.nextval as domain_map_id, t1.manifest_id, t1.data_partner_id, 'PROVIDER' as DOMAIN_NAME, 
        pr.PROVIDER_ID as source_id, 
        concat( t1.data_partner_id, pr.PROVIDER_ID ) as N3C_ID, sysdate as create_date 
        from CDMH_STAGING.manifest t1 
        cross join 
        NATIVE_OMOP522_CDM.PROVIDER pr  ;--126,671 rows inserted.
