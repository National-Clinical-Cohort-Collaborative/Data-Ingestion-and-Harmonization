
CREATE PROCEDURE                                                                  CDMH_STAGING.SP_P2O_SRC_OBS_GEN (
    datapartnerid   IN    NUMBER,
    manifestid      IN    NUMBER,
    recordcount     OUT   NUMBER
) AS 
/******************************************************************************************************************************************************
     Name:      SP_P2O_SRC_OBS_GEN
     Purpose:    Loading The NATIVE_PCORNET51_CDM.OBS_GEN Table into 
                1. CDMH_STAGING.ST_OMOP53_PROCEDURE_OCCURRENCE - ventilation data
     Source Revisions:
     Edit History : Revisions:
     Ver         Date        Author              Description
     0.1         8/30/20     SHONG               Intial Version.
                                                 Insert ventilation data to artificial respiration procedure_occurrence domain with 4230167 concept id
     0.2         8/31/20     DIH                 Generate visit occurrence record and use that value in procedure occurrence for artifical respiration procedure
     0.3         9/09/2020    DIH                 Updated the *_type_concept_id logic
     0.4         9/28/2020  SHONG                For the ICUVISIT inserts from OBS_GEN src should check for OBSGEN_CODE=2000
******************************************************************************************************************************************************/
    proc_recordcount    NUMBER;
    visit_recordcount   NUMBER;
BEGIN
    DELETE FROM cdmh_staging.st_omop53_visit_occurrence
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_OBS_GEN';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_visit_occurrence (
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
            datapartnerid           AS data_partner_id,
            manifestid              AS manifest_id,
            e.n3cds_domain_map_id   AS visit_occurrence_id,
            p.n3cds_domain_map_id   AS person_id,
            581379 AS visit_concept_id,
            obg.obsgen_date         AS visit_start_date,
            NULL AS visit_start_datetime  ---( obsgen_date, obsgent_time)
            ,
            obg.obsgen_date         AS visit_end_date,
            to_date(to_char(obg.obsgen_date,'YYYY-MM-DD') || ' ' || obg.obsgen_time,'YYYY-MM-DD HH24:MI')     AS visit_end_datetime,
            32831 AS visit_type_concept_id,
            NULL provider_id,
            NULL care_site_id,
            'obsgen_type:'
            || obg.obsgen_type
            || ' obsgen_code:'
            || obg.obsgen_code
            || ' obsgen_source: '
            || obg.obsgen_source
            || ' obsgen_result_text:'
            || obsgen_result_text AS visit_source_value,
            581379 visit_source_concept_id,
            32833 AS admitting_source_concept_id,
            obg.obsgen_source       AS admitting_source_value,
            NULL AS discharge_to_concept_id,
            NULL AS discharge_to_source_value,
            NULL AS preceding_visit_occurrence_id,
            'PCORNET_OBS_GEN' AS domain_source
        FROM
            native_pcornet51_cdm.obs_gen    obg
            JOIN cdmh_staging.person_clean       pc ON obg.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map   p ON p.source_id = obg.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map   e ON e.source_id = obg.obsgenid
                                                         AND e.domain_name = 'OBS_GEN'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
        WHERE
            obg.obsgen_type = 'PC_COVID'
            AND obg.obsgen_code = 2000
            AND obg.obsgen_source = 'DR'
            AND obg.obsgen_result_text = 'Y';

    visit_recordcount := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_procedure_occurrence
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'PCORNET_OBS_GEN';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_procedure_occurrence (
        data_partner_id,
        manifest_id,
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        modifier_concept_id,
        quantity,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        procedure_source_value,
        procedure_source_concept_id,
        modifier_source_value,
        domain_source
    )
        SELECT
            datapartnerid            AS data_partner_id,
            manifestid               AS manifest_id,
            mp.n3cds_domain_map_id   AS procedure_occurrence_id,
            p.n3cds_domain_map_id    AS person_id,
            4230167 AS procedure_concept_id,----------- artificial respiration concept id
            obg.obsgen_date          AS procedure_date,
            NULL AS procedure_datetime,
--            38000275 AS procedure_type_concept_id, -- use this type concept id for ehr order list
            32817 AS procedure_type_concept_id, 
            0 modifier_concept_id, -- need to create a cpt_concept_id table based on the source_code_concept id
            NULL AS quantity,
            NULL AS provider_id,
            e.n3cds_domain_map_id    AS visit_occurrence_id,
            NULL AS visit_detail_id,
            obg.obsgen_type
            || obg.obsgen_code
            || obg.obsgen_source
            || obg.obsgen_result_text AS procedure_source_value,
            38000275 AS procedure_source_concept_id,
            NULL AS modifier_source_value,
            'PCORNET_OBS_GEN' AS domain_source
        FROM
            native_pcornet51_cdm.obs_gen    obg
            JOIN cdmh_staging.person_clean       pc ON obg.patid = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map   mp ON mp.source_id = obg.obsgenid
                                                     AND mp.domain_name = 'OBS_GEN'
                                                     AND mp.target_domain_id = 'Procedure'
                                                     AND mp.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map   p ON p.source_id = obg.patid
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map   e ON e.source_id = obg.obsgenid
                                                         AND e.domain_name = 'OBS_GEN'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
                                                         AND e.target_concept_id = 581379
        WHERE
            obg.obsgen_type = 'PC_COVID'
            AND obg.obsgen_code = 3000
            AND obg.obsgen_source = 'DR'
            AND obg.obsgen_result_text = 'Y';

    proc_recordcount := SQL%rowcount;
    COMMIT;
    recordcount := proc_recordcount + visit_recordcount;
    dbms_output.put_line(recordcount || '  PCORnet OBS_GEN source data inserted to st_omop53_procedure_occurrence and st_omop53_visit_occurrence staging table successfully.'
    );
END sp_p2o_src_obs_gen;
