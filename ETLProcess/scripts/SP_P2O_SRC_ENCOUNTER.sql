CREATE PROCEDURE                                                                                            CDMH_STAGING.SP_P2O_SRC_ENCOUNTER (
    datapartnerid   IN    NUMBER,
    manifestid      IN    NUMBER,
    recordcount     OUT   NUMBER
) AS 
/********************************************************************************************************
     Name:      SP_P2O_SRC_ENCOUNTER
     Purpose:    Loading The NATIVE_PCORNET51_CDM.ENCOUNTER Table into 
                1. CDMH_STAGING.ST_OMOP53_VISIT_OCCURRENCE
                2. CDMH_STAGING.ST_OMOP53_CARE_SITE

     Edit History :
     Ver          Date        Author               Description
    0.1       5/16/2020     SHONG               Initial Version
    0.2.     >9/24/2020     DIH                 remove care_sit duplicate entries.
    0.3.      9/24/2020     SHONG, SNAREDLA     Added logic for adding death records based on discharge_status='EX' 
                                                and Observation records based on discharge_status=AW/HO/IP
    0.4       10/02/2020    SHONG               Insert payer plan info to payer_plan_period domain   
    0.5       10/06/2020    SHONG               Remove Condition join/related fields from Death records
    0.6       10/13/2020    DIH                 Add observations records for discharge status EX
*********************************************************************************************************/
    enccnt        NUMBER;
    caresitecnt   NUMBER;
    deathcnt      NUMBER;
    amobscnt      NUMBER;
    obscnt        NUMBER;
    payerPlancnt  NUMBER;  
    exobscnt      NUMBER;
BEGIN
    DELETE FROM cdmh_staging.st_omop53_visit_occurrence
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_ENCOUNTER';

    COMMIT;
    INSERT INTO st_omop53_visit_occurrence (
        data_partner_id,
        manifest_id,
        visit_occurrence_id,
        person_id,
        visit_concept_id,
        visit_start_date,
        visit_start_datetime,
        visit_end_date,
        visit_end_datetime,
        visit_type_concept_id,
        provider_id,
        care_site_id,
        visit_source_value,
        visit_source_concept_id,
        admitting_source_concept_id,
        admitting_source_value,
        discharge_to_concept_id,
        discharge_to_source_value,
        preceding_visit_occurrence_id,
        domain_source
    )
        SELECT
            datapartnerid             AS data_partner_id,
            manifestid                AS manifest_id,
            mp.n3cds_domain_map_id    AS visit_occurrence_id,
            p.n3cds_domain_map_id     AS person_id,
            vx.target_concept_id      AS visit_concept_id,
            enc.admit_date            AS visit_start_date,
            enc.admit_date            AS visit_start_datetime,
            nvl(enc.discharge_date, enc.admit_date) AS visit_end_date,
            nvl(enc.discharge_date, enc.admit_date) AS visit_end_datetime,
        -- confirmed this issue:
        ---Stephanie Hong 6/19/2020 -32035 -default to 32035 "Visit derived from EHR encounter record.
        ---case when enc.enc_type in ('ED', 'AV', 'IP', 'EI') then 38000251  -- need to check this with Charles / missing info
        ---when enc.enc_type in ('OT', 'OS', 'OA') then 38000269
        ---else 0 end AS VISIT_TYPE_CONCEPT_ID,  --check with SMEs
            32035 AS visit_type_concept_id, ---- where did the record came from / need clarification from SME
            prv.n3cds_domain_map_id   AS provider_id,
            cs.n3cds_domain_map_id    AS care_site_id,
            enc.enc_type              AS visit_source_value,
            NULL AS visit_source_concept_id,
            vsrc.target_concept_id    AS admitting_source_concept_id,
            enc.admitting_source      AS admitting_source_value,
            disp.target_concept_id    AS discharge_to_concept_id,
            enc.discharge_status      AS discharge_to_source_value,
            NULL AS preceding_visit_occurrence_id, ---
            'PCORNET_ENCOUNTER' AS domain_source
        FROM
            native_pcornet51_cdm.encounter            enc
            JOIN cdmh_staging.person_clean                 pc ON enc.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map             mp ON mp.source_id = enc.encounterid
                                                     AND mp.domain_name = 'ENCOUNTER'
                                                     AND mp.target_domain_id = 'Visit'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map             p ON p.source_id = enc.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map             prv ON prv.source_id = enc.providerid
                                                           AND prv.domain_name = 'PROVIDER'
                                                           AND prv.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.p2o_facility_type_xwalk      ftx ON ftx.cdm_source = 'PCORnet'
                                                                  AND ftx.cdm_tbl = 'ENCOUNTER'
                                                                  AND ftx.src_facility_type = enc.facility_type
            LEFT JOIN cdmh_staging.n3cds_domain_map             cs ON cs.source_id = enc.encounterid
                                                          AND cs.domain_name = 'ENCOUNTER'
                                                          AND cs.target_domain_id = 'Care_Site'
                                                          AND ftx.target_concept_id = cs.target_concept_id
            LEFT JOIN cdmh_staging.visit_xwalk                  vx ON vx.cdm_tbl = 'ENCOUNTER'
                                                     AND vx.cdm_name = 'PCORnet'
                                                     AND vx.src_visit_type = nvl(TRIM(enc.enc_type), 'UN')
            LEFT JOIN cdmh_staging.p2o_admitting_source_xwalk   vsrc ON vx.cdm_tbl = 'ENCOUNTER'
                                                                      AND vx.cdm_name = 'PCORnet'
                                                                      AND vsrc.src_admitting_source_type = enc.admitting_source
            LEFT JOIN cdmh_staging.p2o_discharge_status_xwalk   disp ON disp.cdm_tbl = 'ENCOUNTER'
                                                                      AND disp.cdm_source = 'PCORnet'
                                                                      AND disp.src_discharge_status = enc.discharge_status;

    enccnt := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_care_site
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_ENCOUNTER';

    COMMIT;
    ---encounter to care_site
    INSERT INTO cdmh_staging.st_omop53_care_site (
        data_partner_id,
        manifest_id,
        care_site_id,
        care_site_name,
        place_of_service_concept_id,
        location_id,
        care_site_source_value,
        place_of_service_source_value,
        domain_source
    ) --8
        SELECT
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS care_site_id,
            NULL AS care_site_name,
            mp.target_concept_id     AS place_of_service_concept_id,
            NULL AS location_id,
            substr(enc.facility_type, 1, 50) AS care_site_source_value,
            substr(enc.facility_type, 1, 50) AS place_of_service_source_value,  -- ehr/encounter
            'PCORNET_ENCOUNTER' AS domain_source
        FROM
            (
                SELECT DISTINCT
                    facility_type
                FROM
                    "NATIVE_PCORNET51_CDM"."ENCOUNTER"
                WHERE
                    encounter.facility_type IS NOT NULL
            ) enc
            JOIN cdmh_staging.p2o_facility_type_xwalk   fx ON fx.cdm_tbl = 'ENCOUNTER'
                                                            AND fx.cdm_source = 'PCORnet'
                                                            AND fx.src_facility_type = enc.facility_type
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = enc.facility_type
                                                     AND mp.domain_name = 'ENCOUNTER'
                                                     AND mp.target_domain_id = 'Care_Site'
                                                     AND fx.target_concept_id = mp.target_concept_id
                                                     AND mp.data_partner_id = datapartnerid;

    caresitecnt := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_death
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_ENCOUNTER';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_death (
        data_partner_id,
        manifest_id,
        person_id,
        death_date,
        death_datetime,
        death_type_concept_id,
        cause_concept_id,
        cause_source_value,
        cause_source_concept_id,
        domain_source
    )
        SELECT
            data_partner_id,
            manifest_id,
            person_id,
            death_date,
            death_datetime,
            death_type_concept_id,
            cause_concept_id,
            cause_source_value,
            cause_source_concept_id,
            domain_source
        FROM
            (
                SELECT
                    datapartnerid           AS data_partner_id,
                    manifestid              AS manifest_id,
                    p.n3cds_domain_map_id   AS person_id,
                    nvl(d.discharge_date, d.admit_date) AS death_date,
                    nvl(d.discharge_date, d.admit_date) AS death_datetime,
                    32823 AS death_type_concept_id,
--                    cs.target_concept_id    AS cause_concept_id,
--                    c.condition_source      AS cause_source_value,
--                    nvl(cs.source_code_concept_id, 0) AS cause_source_concept_id,
                    null       AS cause_concept_id,
                    null       AS cause_source_value,
                    null       AS cause_source_concept_id,
                    'PCORNET_ENCOUNTER' AS domain_source,
                    ROW_NUMBER() OVER(
                        PARTITION BY d.patid
                        ORDER BY
                            d.discharge_date DESC
                    ) rn
                FROM
                    native_pcornet51_cdm.encounter         d
                    JOIN cdmh_staging.person_clean              pc ON d.patid = pc.person_id
                                                         AND pc.data_partner_id = datapartnerid
                    JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = d.patid
                                                            AND p.domain_name = 'PERSON'
                                                            AND p.data_partner_id = datapartnerid
                    LEFT JOIN native_pcornet51_cdm.death             dc ON dc.patid = d.patid
--                    LEFT JOIN native_pcornet51_cdm.condition         c ON c.encounterid = d.encounterid
--                                                                  AND c.patid = d.patid
--                    LEFT JOIN cdmh_staging.p2o_code_xwalk_standard   cs ON cs.cdm_tbl = 'CONDITION'
--                                                                         AND c.condition_type = cs.src_code_type
--                                                                         AND c.condition = cs.source_code
                WHERE
                    discharge_status = 'EX'
                    AND dc.patid IS NULL
            ) cte_ex
        WHERE
            cte_ex.rn = 1;

    deathcnt := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_observation
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_ENCOUNTER';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_observation (
        data_partner_id,
        manifest_id,
        observation_id,
        person_id,
        observation_concept_id,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_number,
        value_as_string,
        value_as_concept_id,
        qualifier_concept_id,
        unit_concept_id,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        observation_source_value,
        observation_source_concept_id,
        unit_source_value,
        qualifier_source_value,
        domain_source
    )
        SELECT
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS observation_id,
            p.n3cds_domain_map_id    AS person_id,
            4021968 AS observation_concept_id,
            NVL(d.discharge_date,d.admit_date)         AS observation_date,
            NVL(d.discharge_date,d.admit_date)         AS observation_datetime,
            32823 AS observation_type_concept_id,
            NULL AS value_as_number,
            NULL AS value_as_string,
            NULL AS value_as_concept_id,
            NULL AS qualifier_concept_id,
            NULL AS unit_concept_id,
            NULL AS provider_id,
            v.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            'Discharge Status-AM' AS observation_source_value,
            44814692 AS observation_source_concept_id,
            NULL AS unit_source_value,
            NULL AS qualifier_source_value,
            'PCORNET_ENCOUNTER' domain_source
        FROM
            native_pcornet51_cdm.encounter   d
            JOIN cdmh_staging.person_clean        pc ON d.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map    p ON p.source_id = d.patid
                                                    AND p.domain_name = 'PERSON'
                                                    AND p.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map    mp ON mp.source_id = d.encounterid
                                                     AND mp.domain_name = 'ENCOUNTER'
                                                     AND mp.target_domain_id = 'Observation'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND mp.target_concept_id = 4021968
            JOIN cdmh_staging.n3cds_domain_map    v ON v.source_id = d.encounterid
                                                    AND v.domain_name = 'ENCOUNTER'
                                                    AND v.target_domain_id = 'Visit'
                                                    AND v.data_partner_id = datapartnerid
        WHERE
            d.discharge_status = 'AM';

    amobscnt := SQL%rowcount;
    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_observation (
        data_partner_id,
        manifest_id,
        observation_id,
        person_id,
        observation_concept_id,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_number,
        value_as_string,
        value_as_concept_id,
        qualifier_concept_id,
        unit_concept_id,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        observation_source_value,
        observation_source_concept_id,
        unit_source_value,
        qualifier_source_value,
        domain_source
    )
        SELECT
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS observation_id,
            p.n3cds_domain_map_id    AS person_id,
            4137274 AS observation_concept_id,
            NVL(d.discharge_date,d.admit_date)         AS observation_date,
            NVL(d.discharge_date,d.admit_date)         AS observation_datetime,
            32823 AS observation_type_concept_id,
            NULL AS value_as_number,
            NULL AS value_as_string,
            NULL AS value_as_concept_id,
            NULL AS qualifier_concept_id,
            NULL AS unit_concept_id,
            NULL AS provider_id,
            v.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            CASE
                WHEN d.discharge_status = 'AW'   THEN
                    'Discharge Status-AW'
                WHEN d.discharge_status = 'HO'   THEN
                    'Discharge Status-HO'
                WHEN d.discharge_status = 'IP'   THEN
                    'Discharge Status-IP'
            END AS observation_source_value,
            CASE
                WHEN d.discharge_status = 'AW'   THEN
                    306685000
                WHEN d.discharge_status = 'HO'   THEN
                    44814696
                WHEN d.discharge_status = 'IP'   THEN
                    44814698
            END AS observation_source_concept_id,
            NULL AS unit_source_value,
            NULL AS qualifier_source_value,
            'PCORNET_ENCOUNTER' domain_source
        FROM
            native_pcornet51_cdm.encounter   d
            JOIN cdmh_staging.person_clean        pc ON d.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map    p ON p.source_id = d.patid
                                                    AND p.domain_name = 'PERSON'
                                                    AND p.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map    mp ON mp.source_id = d.encounterid
                                                     AND mp.domain_name = 'ENCOUNTER'
                                                     AND mp.target_domain_id = 'Observation'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND mp.target_concept_id = 4137274
            JOIN cdmh_staging.n3cds_domain_map    v ON v.source_id = d.encounterid
                                                    AND v.domain_name = 'ENCOUNTER'
                                                    AND v.target_domain_id = 'Visit'
                                                    AND v.data_partner_id = datapartnerid
        WHERE
            d.discharge_status IN (
                'AW',
                'HO',
                'IP'
            );

    obscnt := SQL%rowcount;
    COMMIT;
    
     DELETE FROM cdmh_staging.ST_OMOP53_PAYER_PLAN_PERIOD
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_ENCOUNTER';

    COMMIT;   
    
    INSERT INTO CDMH_STAGING.ST_OMOP53_PAYER_PLAN_PERIOD
(
DATA_PARTNER_ID,
MANIFEST_ID,
PAYER_PLAN_PERIOD_ID,
PERSON_ID,
PAYER_PLAN_PERIOD_START_DATE,
PAYER_PLAN_PERIOD_END_DATE,
PAYER_CONCEPT_ID,
PAYER_SOURCE_VALUE,
PAYER_SOURCE_CONCEPT_ID,
PLAN_CONCEPT_ID,
PLAN_SOURCE_VALUE,
PLAN_SOURCE_CONCEPT_ID,
SPONSOR_CONCEPT_ID,
SPONSOR_SOURCE_VALUE,
SPONSOR_SOURCE_CONCEPT_ID,
FAMILY_SOURCE_VALUE,
STOP_REASON_CONCEPT_ID,
STOP_REASON_SOURCE_VALUE,
STOP_REASON_SOURCE_CONCEPT_ID,
DOMAIN_SOURCE
)
SELECT 
DATA_PARTNER_ID,
MANIFEST_ID,
PAYER_PLAN_PERIOD_ID,
PERSON_ID,
PAYER_PLAN_PERIOD_START_DATE,
PAYER_PLAN_PERIOD_END_DATE,
PAYER_CONCEPT_ID,
PAYER_SOURCE_VALUE,
PAYER_SOURCE_CONCEPT_ID,
PLAN_CONCEPT_ID,
PLAN_SOURCE_VALUE,
PLAN_SOURCE_CONCEPT_ID,
SPONSOR_CONCEPT_ID,
SPONSOR_SOURCE_VALUE,
SPONSOR_SOURCE_CONCEPT_ID,
FAMILY_SOURCE_VALUE,
STOP_REASON_CONCEPT_ID,
STOP_REASON_SOURCE_VALUE,
STOP_REASON_SOURCE_CONCEPT_ID,
DOMAIN_SOURCE
FROM (

SELECT /*+ use_hash */
    datapartnerid AS DATA_PARTNER_ID,
    manifestid AS MANIFEST_ID,
    mp.N3CDS_DOMAIN_MAP_ID as payer_plan_period_id,
    p.N3CDS_DOMAIN_MAP_ID as person_id,
    e.ADMIT_DATE as payer_plan_period_start_date,
    case when e.DISCHARGE_DATE is not null then e.DISCHARGE_DATE
        else e.ADMIT_DATE
        end as payer_plan_period_end_date,
    xw.target_concept_id as payer_concept_id, --get the list of OMOP concept_ids
    'PAYER_TYPE_PRIMARY ' || e.PAYER_TYPE_PRIMARY as payer_source_value, --this one can stay as the PCORnet source value
    null as payer_source_concept_id,
    null as plan_concept_id,
    null as plan_source_value,
    null as plan_source_concept_id,
    null as sponsor_concept_id,
    null as sponsor_source_value,
    null as sponsor_source_concept_id,
    null as family_source_value,
    null as stop_reason_concept_id,
    null as stop_reason_source_value,
    null as stop_reason_source_concept_id,
    'PCORNET_ENCOUNTER' AS DOMAIN_SOURCE,
    row_number() over (partition by e.patid,to_char(admit_date,'YYYY-MM-DD'),nvl(to_char(discharge_date,'YYYY-MM-DD'),to_char(admit_date,'YYYY-MM-DD')),e.PAYER_TYPE_PRIMARY 
    order by admit_date desc,nvl(admit_date,discharge_date) desc, encounterid
    )
    as rnk
FROM
    NATIVE_PCORNET51_CDM.ENCOUNTER e
    JOIN CDMH_STAGING.PERSON_CLEAN pc ON e.PATID=pc.PERSON_ID 
                                AND pc.DATA_PARTNER_ID=datapartnerid 
    JOIN CDMH_STAGING.P2O_TERM_XWALK xw on xw.SRC_CODE=e.payer_type_primary 
                                AND xw.cdm_tbl_column_name= 'PAYER_TYPE_PRIMARY'  
    JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP p ON p.SOURCE_ID=e.PATID AND p.DATA_PARTNER_ID=datapartnerid AND p.domain_name='PERSON'                            
    JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on e.ENCOUNTERID=Mp.Source_Id
                                AND mp.DOMAIN_NAME='ENCOUNTER' 
                                AND Mp.Target_Domain_Id = 'PAYER_TYPE_PRIMARY' 
                                AND mp.DATA_PARTNER_ID=datapartnerid   
                                AND mp.TARGET_CONCEPT_ID=xw.TARGET_CONCEPT_ID
                                
WHERE
    payer_type_primary is not null 

UNION ALL
 
--secondary payors
SELECT /*+ use_hash */
    datapartnerid AS DATA_PARTNER_ID,
    manifestid AS MANIFEST_ID,
    mp.N3CDS_DOMAIN_MAP_ID as payer_plan_period_id,
    p.N3CDS_DOMAIN_MAP_ID as person_id,
    e.ADMIT_DATE as payer_plan_period_start_date,
    case when e.DISCHARGE_DATE is not null then e.DISCHARGE_DATE
        else e.ADMIT_DATE
        end as payer_plan_period_end_date,
    xw.target_concept_id as payer_concept_id, --get the list of the OMOP concept_ids
    'PAYER_TYPE_SECONDARY ' || e.PAYER_TYPE_SECONDARY as payer_source_value, --this one can stay as the PCORnet source value
    null as payer_source_concept_id,
    null as plan_concept_id,
    null as plan_source_value,
    null as plan_source_concept_id,
    null as sponsor_concept_id,
    null as sponsor_source_value,
    null as sponsor_source_concept_id,
    null as family_source_value,
    null as stop_reason_concept_id,
    null as stop_reason_source_value,
    null as stop_reason_source_concept_id,
    'PCORNET_ENCOUNTER' AS DOMAIN_SOURCE,
    row_number() over (partition by e.patid,to_char(admit_date,'YYYY-MM-DD'),nvl(to_char(discharge_date,'YYYY-MM-DD'),to_char(admit_date,'YYYY-MM-DD')),e.PAYER_TYPE_SECONDARY 
    order by admit_date desc,nvl(admit_date,discharge_date) desc, encounterid
    )
    as rnk
FROM
    NATIVE_PCORNET51_CDM.ENCOUNTER e
    JOIN CDMH_STAGING.PERSON_CLEAN pc ON e.PATID=pc.PERSON_ID 
                                AND pc.DATA_PARTNER_ID=datapartnerid 
    JOIN CDMH_STAGING.P2O_TERM_XWALK xw on xw.SRC_CODE=e.payer_type_secondary 
                                AND xw.cdm_tbl_column_name= 'PAYER_TYPE_SECONDARY'  
    JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP p ON p.SOURCE_ID=e.PATID AND p.DATA_PARTNER_ID=datapartnerid AND p.domain_name='PERSON'                            
    JOIN CDMH_STAGING.N3CDS_DOMAIN_MAP mp on e.ENCOUNTERID=Mp.Source_Id 
                                AND mp.DOMAIN_NAME='ENCOUNTER' 
                                AND Mp.Target_Domain_Id = 'PAYER_TYPE_SECONDARY' 
                                AND mp.DATA_PARTNER_ID=datapartnerid   
                                AND mp.TARGET_CONCEPT_ID=xw.TARGET_CONCEPT_ID
WHERE
    payer_type_secondary is not null
   ) Payer 
   where rnk=1
    ;
    payerPlancnt :=sql%rowcount;
    COMMIT;
    
        INSERT INTO cdmh_staging.st_omop53_observation (
        data_partner_id,
        manifest_id,
        observation_id,
        person_id,
        observation_concept_id,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_number,
        value_as_string,
        value_as_concept_id,
        qualifier_concept_id,
        unit_concept_id,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        observation_source_value,
        observation_source_concept_id,
        unit_source_value,
        qualifier_source_value,
        domain_source
    )
        SELECT
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS observation_id,
            p.n3cds_domain_map_id    AS person_id,
            4216643 AS observation_concept_id,
            NVL(d.discharge_date,d.admit_date)         AS observation_date,
            NVL(d.discharge_date,d.admit_date)         AS observation_datetime,
            44818516 AS observation_type_concept_id,
            NULL AS value_as_number,
            NULL AS value_as_string,
            NULL AS value_as_concept_id,
            NULL AS qualifier_concept_id,
            NULL AS unit_concept_id,
            NULL AS provider_id,
            v.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            'Discharge Status-EX' AS observation_source_value,
            4216643 AS observation_source_concept_id,
            NULL AS unit_source_value,
            NULL AS qualifier_source_value,
            'PCORNET_ENCOUNTER' domain_source
        FROM
            native_pcornet51_cdm.encounter   d
            JOIN cdmh_staging.person_clean        pc ON d.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map    p ON p.source_id = d.patid
                                                    AND p.domain_name = 'PERSON'
                                                    AND p.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map    mp ON mp.source_id = d.encounterid
                                                     AND mp.domain_name = 'ENCOUNTER'
                                                     AND mp.target_domain_id = 'Observation'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND mp.target_concept_id = 4216643
            JOIN cdmh_staging.n3cds_domain_map    v ON v.source_id = d.encounterid
                                                    AND v.domain_name = 'ENCOUNTER'
                                                    AND v.target_domain_id = 'Visit'
                                                    AND v.data_partner_id = datapartnerid
        WHERE
            d.discharge_status = 'EX';
            exobscnt:=sql%rowcount;
            COMMIT;
    
    recordcount := enccnt + caresitecnt + deathcnt + amobscnt + obscnt+payerPlancnt+exobscnt;
    dbms_output.put_line(recordcount || '  PCORnet ENCOUNTER source data inserted to ENCOUNTER staging table, ST_OMOP53_VISIT_OCCURRENCE, and ST_OMOP53_CARE_SITE if facility type is not null, and ST_OMOP53_DEATH, and ST_OMOP53_OBSERVATION, and ST_OMOP53_PAYER_PLAN_PERIOD  successfully.'
    );
END sp_p2o_src_encounter;
