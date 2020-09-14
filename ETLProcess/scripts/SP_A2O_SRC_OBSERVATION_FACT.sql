
CREATE PROCEDURE                             CDMH_STAGING.SP_A2O_SRC_OBSERVATION_FACT (
    datapartnerid   IN    NUMBER,
    manifestid      IN    NUMBER,
    recordcount     OUT   NUMBER
) AS
/********************************************************************************************************
     Name:      sp_a2o_src_observation_fact
     Purpose:    Loading The native_i2b2act_cdm.observation_fact Table into 
                1. CDMH_Staging.ST_OMOP53_Condition
                2. CDMH_Staging.ST_OMOP53_Procedure
                3. CDMH_Staging.ST_OMOP53_Measurement
                4. CDMH_Staging.ST_OMOP53_OBSERVATION
                5. CDMH_Staging.ST_OMOP53_DRUG_EXPOSURE
     Source:
     Revisions:
     Ver       Date      Author    Description
     0.1       7/21/20   SHONG     Initial version
     0.2       7/28/20   SHONG      Default mapped value for measurement_type is incorrect 
                                    - changed to use 5001 as ehr order measurement_type_concept_id 
     0.3       7/29/20   SHONG     Added drug target domain data  
     0.4       8/3/20    SHONG     Update Measurement logic for operator_concept_id,operator_concept_id
                                   , range_high and range_low
     0.5       8/21/20   DIH       Update Measurement unit_concept_id, range_high and range_low to null
     0.6       8/24/20   DIH       Added drug_exposure query when concept_cd='ACT|LOCAL:REMDESIVIR'
     0.7       8/29/20   SHONG Updated to include the following tests:  UMLS:C1335447%' 'UMLS:C1611271%' 'UMLS:C4303880%' 'UMLS:C1334932%'

*********************************************************************************************************/
    conditioncnt     NUMBER;
    procedurecnt     NUMBER;
    measurementcnt   NUMBER;
    observationcnt   NUMBER;
    drugcnt          NUMBER;
    drugcnt1         NUMBER;
    measurementcnt1  NUMBER ;
BEGIN
    DELETE FROM cdmh_staging.st_omop53_condition_occurrence
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'I2B2ACT_OBSERVATION_FACT';

    COMMIT;
    INSERT INTO cdmh_staging.st_omop53_condition_occurrence (
        data_partner_id,
        manifest_id,
        condition_occurrence_id,
        person_id,
        condition_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_end_date,
        condition_end_datetime,
        condition_type_concept_id,
        stop_reason,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        condition_source_value,
        condition_source_concept_id,
        condition_status_source_value,
        condition_status_concept_id,
        domain_source
    )
        SELECT
                /*+ 
        INDEX(xw,IDX_A2O_CODE_XWALK_2)
         index(pc,IDX_PERSON_CLEAN) 
         index(o,IDX_ACT_OBS_FACT)
        */ 
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS condition_occurrence_id,
            p.n3cds_domain_map_id       AS person_id,
            mp.target_concept_id        AS condition_concept_id,
            o.start_date                AS condition_start_date,
            o.start_date                AS condition_start_datetime,
            nvl(o.end_date, o.start_date) AS condition_end_date,
            NULL AS condition_end_datetime,
            43542353 AS condition_type_concept_id, --already collected from the visit_occurrence_table. / visit_occurrence.visit_source_value 
            NULL AS stop_reason, ---- encounter discharge type e.discharge type
            NULL AS provider_id, ---is provider linked to patient
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            o.concept_cd                AS condition_source_value,
            xw.source_code_concept_id   AS condition_source_concept_id,
            NULL AS condition_status_source_value,
            NULL AS condition_status_concept_id,
            'I2B2ACT_OBSERVATION_FACT' AS domain_source
        FROM
            native_i2b2act_cdm.observation_fact    o
            JOIN cdmh_staging.person_clean              pc ON o.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = o.observation_fact_id
                                                     AND mp.domain_name = 'OBSERVATION_FACT'
                                                     AND mp.target_domain_id = 'Condition'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = pc.person_id
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = o.encounter_num
                                                    AND e.domain_name = 'VISIT_DIMENSION'
                                                    AND e.target_domain_id = 'Visit'
                                                    AND e.data_partner_id = datapartnerid 
    --LEFT JOIN CDMH_STAGING.visit_xwalk vx ON vx.cdm_tbl='VISIT_DIMENSION' AND vx.CDM_NAME='I2B2ACT' AND vx.src_visit_type= o.inout_cd
            LEFT JOIN cdmh_staging.a2o_code_xwalk_standard   xw ON xw.src_code_type
                                                                 || ':'
                                                                 || xw.src_code = o.concept_cd
                                                                 AND xw.cdm_tbl = 'OBSERVATION_FACT'
                                                                 AND xw.target_concept_id = mp.target_concept_id
                                                                 AND xw.target_domain_id = 'Condition';

    conditioncnt := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_procedure_occurrence
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'I2B2ACT_OBSERVATION_FACT';

    COMMIT;
    --procedure
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
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS procedure_occurrence_id,
            p.n3cds_domain_map_id       AS person_id,
            mp.target_concept_id        AS procedure_concept_id, -- use from the map to avoid multi-map issue
            o.start_date                AS procedure_date,
            o.start_date                AS procedure_datetime,
            38000275 AS procedure_type_concept_id, -- ssh: 7/27/20 use this type concept id for ehr order list for ACT 
            0 modifier_concept_id, -- need to create a cpt_concept_id table based on the source_code_concept id
            NULL AS quantity,
            NULL AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            xw.src_code                 AS procedure_source_value,
            xw.source_code_concept_id   AS procedure_source_concept_id,
            xw.src_code_type            AS modifier_source_value,
            'I2B2ACT_OBSERVATION_FACT' AS domain_source
        FROM
            native_i2b2act_cdm.observation_fact    o
            JOIN cdmh_staging.person_clean              pc ON o.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = o.observation_fact_id
                                                     AND mp.domain_name = 'OBSERVATION_FACT'
                                                     AND mp.target_domain_id = 'Procedure'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = pc.person_id
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = o.encounter_num
                                                    AND e.domain_name = 'VISIT_DIMENSION'
                                                    AND e.target_domain_id = 'Visit'
                                                    AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.a2o_code_xwalk_standard   xw ON xw.src_code_type
                                                                 || ':'
                                                                 || xw.src_code = o.concept_cd
                                                                 AND xw.cdm_tbl = 'OBSERVATION_FACT'
                                                                 AND xw.target_domain_id = 'Procedure'
                                                                 AND xw.target_concept_id = mp.target_concept_id;

    procedurecnt := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_measurement
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'I2B2ACT_OBSERVATION_FACT';

    COMMIT;
  --measurement 
    INSERT INTO cdmh_staging.st_omop53_measurement (--for observation_fact target_domain_id = measurement
        data_partner_id,
        manifest_id,
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_date,
        measurement_datetime,
        measurement_time,
        measurement_type_concept_id,
        operator_concept_id,
        value_as_number,
        value_as_concept_id,
        unit_concept_id,
        range_low,
        range_high,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        measurement_source_value,
        measurement_source_concept_id,
        unit_source_value,
        value_source_value,
        domain_source
    )
        SELECT
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS measurement_id,
            p.n3cds_domain_map_id       AS person_id,
            mp.target_concept_id        AS measurement_concept_id, --concept id for lab - measurement/ use from the map to avoid multi-map issue
            o.start_date                AS measurement_date,
            o.start_date                AS measurement_datetime,
            NULL AS measurement_time,
            5001 AS measurement_type_concept_id,  -----3017575 Reference lab test results------------
            ---- operator_concept is only relevant for numeric value 
            CASE
                WHEN valtype_cd = 'N' THEN
                    tvqual.target_concept_id
                ELSE
                    0
            END AS operator_concept_id, -- operator is stored in tval_char / sometimes
            -- ssh/ 7/28/20 - if the value is numerical then store the result in the value_as_number
--            o.nval_num                  AS value_as_number, --result_num
            CASE
                WHEN valtype_cd = 'T' THEN
                    NULL
                WHEN valtype_cd = 'N' THEN
                    o.nval_num
                ELSE
                    NULL
            END AS value_as_number, --result_num
            -----SHONG: if the valtype is categorical use the tval_char, but if tval_char is null then use the valueflag_cd value  
            -----ssh UK: data had null in tval_char and categorical values in the valuelflag_cd 8/1/2020
            CASE
                WHEN valtype_cd = 'T'
                     AND tval_char IS NULL THEN
                    vfqual.target_concept_id
                WHEN valtype_cd = 'T'
                     AND tval_char IS NOT NULL THEN
                    tvqual.target_concept_id
                ELSE
                    NULL
            END AS value_as_concept_id, -------qualitative result
            --mapped unit concept id
            CASE
                WHEN valtype_cd = 'T' THEN
                    null
                ELSE
                    units.target_concept_id
            END AS unit_concept_id,
            --if the valtype_cd = T and concept_cd like 'LOINC:%' tval_char contains 154 variations - updated :
            ---update rules: ssh: 8/3/20
            --- per michele's note set range to 
            null range_low,   --ssh 8/5/20 set to 0 
            null AS range_high, --ssh 8/5/20 set to 0 
            NULL AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            o.concept_cd                AS measurement_source_value,
            --cast(xw.source_code_concept_id as int ) as MEASUREMENT_SOURCE_CONCEPT_ID,
            xw.source_code_concept_id   AS measurement_source_concept_id,
            o.units_cd                  AS unit_source_value,
            ---if numerical value then Concat( tval_char(operator code text like E/NE/LE/L/G/GE) + nval_num + valueflag_cd) 
            ---if categorical value then tval_char contains text and valueflag_cd contains qual result values
            'concept_cd:'
            || concept_cd
            || '|tval_char: '
            || tval_char
            || '|nval_num:'
            || o.nval_num
            || '|valueflag_cd:'
            || valueflag_cd AS value_source_value,
            'I2B2ACT_OBSERVATION_FACT' AS domain_source
        FROM
            native_i2b2act_cdm.observation_fact    o
            JOIN cdmh_staging.person_clean              pc ON o.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = o.observation_fact_id
                                                     AND mp.domain_name = 'OBSERVATION_FACT'
                                                     AND mp.target_domain_id = 'Measurement'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = o.patient_num
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = o.encounter_num
                                                    AND e.domain_name = 'VISIT_DIMENSION'
                                                    AND e.target_domain_id = 'Visit'
                                                    AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.a2o_code_xwalk_standard   xw ON xw.src_code_type
                                                                 || ':'
                                                                 || xw.src_code = o.concept_cd
                                                                 AND xw.cdm_tbl = 'OBSERVATION_FACT'
                                                                 AND xw.target_domain_id = 'Measurement'
                                                                 AND xw.target_concept_id = mp.target_concept_id
            LEFT JOIN cdmh_staging.a2o_term_xwalk            units ON o.units_cd = units.src_code
                                                           AND units.cdm_tbl_column_name = 'UNITS_CD'
            LEFT JOIN cdmh_staging.a2o_term_xwalk            vfqual ON lower(TRIM(o.valueflag_cd)) = lower(TRIM(vfqual.src_code))
                                                            AND vfqual.cdm_tbl = 'OBSERVATION_FACT'
                                                            AND vfqual.cdm_tbl_column_name = 'VALUEFLAG_CD'
            LEFT JOIN cdmh_staging.a2o_term_xwalk            tvqual ON lower(TRIM(o.tval_char)) = lower(TRIM(tvqual.src_code))
                                                            AND tvqual.cdm_tbl = 'OBSERVATION_FACT'
                                                            AND tvqual.cdm_tbl_column_name = 'TVAL_CHAR';

    measurementcnt := SQL%rowcount;
    COMMIT;
    DELETE FROM cdmh_staging.st_omop53_observation
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'I2B2ACT_OBSERVATION_FACT';

    COMMIT;
  --observation fact to observation
  --begin condition to observation 
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

        --observation fact  to observation
        SELECT DISTINCT
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS observation_id,
            p.n3cds_domain_map_id       AS person_id,
            mp.target_concept_id        AS observation_concept_id, -- use xw.target_concept_id from the map
            ob.start_date               AS observation_date,
            ob.start_date               AS observation_datetime,
            38000280 observation_type_concept_id,
            CASE
                WHEN valtype_cd = 'N' THEN
                    ob.nval_num
                WHEN valtype_cd = 'T' THEN
                    NULL
            END AS value_as_number,
            CASE
                WHEN valtype_cd = 'T' THEN
                    ob.tval_char
                WHEN valtype_cd = 'N' THEN
                    NULL
            END AS value_as_string,
            vfqual.target_concept_id    AS value_as_concept_id,
--            0 AS qualifier_concept_id, -- tval_char will contain operator code if valtype_cd is N for numeric
--            0 AS qualifier_concept_id, -- tval_char will contain operator code if valtype_cd is N for numeric
            CASE
                WHEN valtype_cd = 'N'
                     AND tval_char = 'E' THEN
                    4172703 ----4319898
                WHEN valtype_cd = 'N'
                     AND tval_char = 'G' THEN
                    4172704 ---4139823
                WHEN valtype_cd = 'N'
                     AND tval_char = 'L' THEN
                    4171756
                WHEN valtype_cd = 'N'
                     AND tval_char = 'LE' THEN
                    4171754
                WHEN valtype_cd = 'N'
                     AND tval_char = 'GE' THEN
                    4171755
                ELSE
                    0
            END AS qualifier_concept_id, -- tval_char will contain operator code if valtype_cd is N for numeric
            CASE
                WHEN valtype_cd = 'T' THEN
                    0
                ELSE
                    units.target_concept_id
            END AS unit_concept_id,
            NULL AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            ob.concept_cd               AS observation_source_value,
            xw.source_code_concept_id   AS observation_source_concept_id,
            units_cd                    AS unit_source_value,
            valueflag_cd                AS qualifier_source_value,
            'I2B2ACT_OBSERVATION_FACT' domain_source
        FROM
            native_i2b2act_cdm.observation_fact    ob
            JOIN cdmh_staging.person_clean              pc ON ob.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = ob.observation_fact_id
                                                     AND mp.domain_name = 'OBSERVATION_FACT'
                                                     AND mp.target_domain_id = 'Observation'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = ob.patient_num
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = ob.encounter_num
                                                    AND e.domain_name = 'VISIT_DIMENSION'
                                                    AND e.target_domain_id = 'Visit'
                                                    AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.a2o_code_xwalk_standard   xw ON xw.src_code_type
                                                                 || ':'
                                                                 || xw.src_code = ob.concept_cd
                                                                 AND xw.cdm_tbl = 'OBSERVATION_FACT'
                                                                 AND xw.target_domain_id = 'Observation'
                                                                 AND xw.target_concept_id = mp.target_concept_id
            LEFT JOIN cdmh_staging.a2o_term_xwalk            units ON ob.units_cd = units.src_code
                                                           AND units.cdm_tbl_column_name = 'UNITS_CD'
            LEFT JOIN cdmh_staging.a2o_term_xwalk            vfqual ON lower(TRIM(ob.valueflag_cd)) = lower(TRIM(vfqual.src_code))
                                                            AND vfqual.cdm_tbl = 'OBSERVATION_FACT'
                                                            AND vfqual.cdm_tbl_column_name = 'VALUEFLAG_CD';

    observationcnt := SQL%rowcount;
    COMMIT;
    --end observation
    DELETE FROM cdmh_staging.st_omop53_drug_exposure
    WHERE
        data_partner_id = datapartnerid
        AND domain_source = 'I2B2ACT_OBSERVATION_FACT';
    --Drug	270

    INSERT INTO cdmh_staging.st_omop53_drug_exposure (  --INSERT INTO CDMH_STAGING.ST_OMOP53_DRUG_EXPOSURE
        data_partner_id,
        manifest_id,
        drug_exposure_id,
        person_id,
        drug_concept_id,
        drug_exposure_start_date,
        drug_exposure_start_datetime,
        drug_exposure_end_date,
        drug_exposure_end_datetime,
        verbatim_end_date,
        drug_type_concept_id,
        stop_reason,
        refills,
        quantity,
        days_supply,
        sig,
        route_concept_id,
        lot_number,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        drug_source_value,
        drug_source_concept_id,
        route_source_value,
        dose_unit_source_value,
        domain_source
    ) --
  --observation fact to drug
        SELECT /*+ 
            INDEX(xw,IDX_A2O_CODE_XWALK_2)
            index(pc,IDX_PERSON_CLEAN) 
            index(ob,IDX_ACT_OBS_FACT)
        */ 
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS drug_exposure_id,
            p.n3cds_domain_map_id       AS person_id,
            xw.target_concept_id        AS drug_concept_id,
            nvl(start_date, end_date) AS drug_exposure_start_date,
            nvl(start_date, end_date) AS drug_exposure_start_datetime,
            nvl(end_date, start_date) AS drug_exposure_end_date,
            nvl(end_date, start_date) AS drug_exposure_end_datetime,
            end_date                    AS verbatim_end_date,
            38000177 AS drug_type_concept_id,
            NULL AS stop_reason,
            NULL AS refills,
            ob.quantity_num             AS quantity,
            NULL AS days_supply,
            NULL AS sig,
            NULL AS route_concept_id,
            NULL AS lot_number,
            NULL AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            concept_cd                  AS drug_source_value,
            xw.source_code_concept_id   AS drug_source_concept_id, --- drug source concept id if it is prescribing
            ob.modifier_cd              AS route_source_value,
            ob.units_cd                 AS dose_unit_source_value,
            'I2B2ACT_OBSERVATION_FACT' AS domain_source
        FROM
            native_i2b2act_cdm.observation_fact    ob
            JOIN cdmh_staging.person_clean              pc ON ob.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = ob.observation_fact_id
                                                     AND mp.domain_name = 'OBSERVATION_FACT'
                                                     AND mp.target_domain_id = 'Drug'
                                                     AND mp.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = ob.patient_num
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = ob.encounter_num
                                                         AND e.domain_name = 'VISIT_DIMENSION'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
            JOIN cdmh_staging.a2o_code_xwalk_standard   xw ON xw.src_code_type
                                                                 || ':'
                                                                 || xw.src_code = ob.concept_cd
                                                                 AND xw.cdm_tbl = 'OBSERVATION_FACT'
                                                                 AND xw.target_domain_id = 'Drug'
                                                                 AND xw.target_concept_id = mp.target_concept_id
            LEFT JOIN cdmh_staging.a2o_term_xwalk            units ON ob.units_cd = units.src_code
                                                           AND units.cdm_tbl_column_name = 'UNITS_CD'
            LEFT JOIN cdmh_staging.a2o_term_xwalk            rqual ON lower(TRIM(ob.valueflag_cd)) = lower(TRIM(rqual.src_code))
                                                           AND rqual.cdm_tbl = 'OBSERVATION_FACT'
                                                           AND rqual.cdm_tbl_column_name = 'VALUEFLAG_CD';

    drugcnt := SQL%rowcount;
    COMMIT;
    --concept_cd='ACT|LOCAL:REMDESIVIR'
        INSERT INTO cdmh_staging.st_omop53_drug_exposure ( 
        data_partner_id,
        manifest_id,
        drug_exposure_id,
        person_id,
        drug_concept_id,
        drug_exposure_start_date,
        drug_exposure_start_datetime,
        drug_exposure_end_date,
        drug_exposure_end_datetime,
        verbatim_end_date,
        drug_type_concept_id,
        stop_reason,
        refills,
        quantity,
        days_supply,
        sig,
        route_concept_id,
        lot_number,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        drug_source_value,
        drug_source_concept_id,
        route_source_value,
        dose_unit_source_value,
        domain_source
    ) --
  --observation fact to drug
        SELECT  /*+ 
         index(pc,IDX_PERSON_CLEAN) 
         index(units,IDX_A2O_TERM_XWALK_1)
         index(rqual,IDX_A2O_TERM_XWALK_1)
         index(ob,IDX_ACT_OBS_FACT_CONCEPT_CD)
        */ 
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS drug_exposure_id,
            p.n3cds_domain_map_id       AS person_id,
            mp.target_concept_id        AS drug_concept_id,
            nvl(start_date, end_date) AS drug_exposure_start_date,
            nvl(start_date, end_date) AS drug_exposure_start_datetime,
            nvl(end_date, start_date) AS drug_exposure_end_date,
            nvl(end_date, start_date) AS drug_exposure_end_datetime,
            end_date                    AS verbatim_end_date,
            38000177 AS drug_type_concept_id,
            NULL AS stop_reason,
            NULL AS refills,
            ob.quantity_num             AS quantity,
            NULL AS days_supply,
            NULL AS sig,
            NULL AS route_concept_id,
            NULL AS lot_number,
            NULL AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            concept_cd                  AS drug_source_value,
            37499271 AS drug_source_concept_id, 
            ob.modifier_cd              AS route_source_value,
            ob.units_cd                 AS dose_unit_source_value,
            'I2B2ACT_OBSERVATION_FACT' AS domain_source
        FROM
            native_i2b2act_cdm.observation_fact    ob
            JOIN cdmh_staging.person_clean              pc ON ob.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = ob.observation_fact_id
                                                     AND mp.domain_name = 'OBSERVATION_FACT'
                                                     AND mp.target_domain_id = 'Drug'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND mp.target_concept_id=37499271
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = ob.patient_num
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = ob.encounter_num
                                                         AND e.domain_name = 'VISIT_DIMENSION'
                                                         AND e.target_domain_id = 'Visit'
                                                         AND e.data_partner_id = datapartnerid
            LEFT JOIN cdmh_staging.a2o_term_xwalk            units ON ob.units_cd = units.src_code
                                                           AND units.cdm_tbl_column_name = 'UNITS_CD'
            LEFT JOIN cdmh_staging.a2o_term_xwalk            rqual ON lower(TRIM(ob.valueflag_cd)) = lower(TRIM(rqual.src_code))
                                                           AND rqual.cdm_tbl = 'OBSERVATION_FACT'
                                                           AND rqual.cdm_tbl_column_name = 'VALUEFLAG_CD'
            WHERE ob.concept_cd='ACT|LOCAL:REMDESIVIR';


    drugcnt1 := SQL%rowcount;
    COMMIT;
    ----------generic covid test qualitative results 
    ------------------------------------------------
     --measurement 
    INSERT INTO cdmh_staging.st_omop53_measurement (--for observation_fact target_domain_id = measurement
        data_partner_id,
        manifest_id,
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_date,
        measurement_datetime,
        measurement_time,
        measurement_type_concept_id,
        operator_concept_id,
        value_as_number,
        value_as_concept_id,
        unit_concept_id,
        range_low,
        range_high,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        measurement_source_value,
        measurement_source_concept_id,
        unit_source_value,
        value_source_value,
        domain_source
    )
        SELECT
            datapartnerid               AS data_partner_id,
            manifestid                  AS manifest_id,
            mp.n3cds_domain_map_id      AS measurement_id,
            p.n3cds_domain_map_id       AS person_id,
            ---- shong 8/29/20 use : SARS-CoV-2 (COVID-19) RNA [Presence] in Unspecified specimen by NAA with probe detection
            706170        AS measurement_concept_id, --concept id for covid 19 generic lab test result
            o.start_date                AS measurement_date,
            o.start_date                AS measurement_datetime,
            NULL AS measurement_time,
            5001 AS measurement_type_concept_id,  -----3017575 Reference lab test results------------
            ---- operator_concept is only relevant for numeric value 
            CASE
                WHEN valtype_cd = 'N' THEN
                    tvqual.target_concept_id
                ELSE
                    0
            END AS operator_concept_id, -- operator is stored in tval_char / sometimes
            -- ssh/ 7/28/20 - if the value is numerical then store the result in the value_as_number
--            o.nval_num                  AS value_as_number, --result_num
            CASE
                WHEN valtype_cd = 'T' THEN
                    NULL
                WHEN valtype_cd = 'N' THEN
                    o.nval_num
                ELSE
                    NULL
            END AS value_as_number, --result_num
            -----SHONG: if the valtype is categorical use the tval_char, but if tval_char is null then use the valueflag_cd value  
            -----ssh UK: data had null in tval_char and categorical values in the valuelflag_cd 8/1/2020
            CASE
                WHEN valtype_cd = 'T'
                     AND tval_char IS NULL THEN
                    vfqual.target_concept_id
                WHEN valtype_cd = 'T'
                     AND tval_char IS NOT NULL THEN
                    tvqual.target_concept_id
                ELSE
                    NULL
            END AS value_as_concept_id, -------qualitative result
            --mapped unit concept id
            CASE
                WHEN valtype_cd = 'T' THEN
                    null
                ELSE
                    units.target_concept_id
            END AS unit_concept_id,
            --if the valtype_cd = T and concept_cd like 'LOINC:%' tval_char contains 154 variations - updated :
            ---update rules: ssh: 8/3/20
            --- per michele's note set range to 
            null range_low,   --ssh 8/5/20 set to 0 
            null AS range_high, --ssh 8/5/20 set to 0 
            NULL AS provider_id,
            e.n3cds_domain_map_id       AS visit_occurrence_id,
            NULL AS visit_detail_id,
            o.concept_cd                AS measurement_source_value,
            --cast(xw.source_code_concept_id as int ) as MEASUREMENT_SOURCE_CONCEPT_ID,
            706170   AS measurement_source_concept_id,
            o.units_cd                  AS unit_source_value,
            ---if numerical value then Concat( tval_char(operator code text like E/NE/LE/L/G/GE) + nval_num + valueflag_cd) 
            ---if categorical value then tval_char contains text and valueflag_cd contains qual result values
            'concept_cd:'
            || concept_cd
            || '|tval_char: '
            || tval_char
            || '|nval_num:'
            || o.nval_num
            || '|valueflag_cd:'
            || valueflag_cd AS value_source_value,
            'I2B2ACT_OBSERVATION_FACT' AS domain_source
        FROM
            native_i2b2act_cdm.observation_fact    o
            JOIN cdmh_staging.person_clean              pc ON o.patient_num = pc.person_id
                                                 AND pc.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          mp ON mp.source_id = o.observation_fact_id
                                                     AND mp.domain_name = 'OBSERVATION_FACT'
                                                     AND mp.target_domain_id = 'Measurement'
                                                     AND mp.data_partner_id = datapartnerid
                                                     AND mp.target_concept_id=706170
            LEFT JOIN cdmh_staging.n3cds_domain_map          p ON p.source_id = o.patient_num
                                                         AND p.domain_name = 'PERSON'
                                                         AND p.data_partner_id = datapartnerid
            JOIN cdmh_staging.n3cds_domain_map          e ON e.source_id = o.encounter_num
                                                    AND e.domain_name = 'VISIT_DIMENSION'
                                                    AND e.target_domain_id = 'Visit'
                                                    AND e.data_partner_id = datapartnerid
            -------
            LEFT JOIN cdmh_staging.a2o_term_xwalk            units ON o.units_cd = units.src_code
                                                           AND units.cdm_tbl_column_name = 'UNITS_CD'
            LEFT JOIN cdmh_staging.a2o_term_xwalk            vfqual ON lower(TRIM(o.valueflag_cd)) = lower(TRIM(vfqual.src_code))
                                                            AND vfqual.cdm_tbl = 'OBSERVATION_FACT'
                                                            AND vfqual.cdm_tbl_column_name = 'VALUEFLAG_CD'
            LEFT JOIN cdmh_staging.a2o_term_xwalk            tvqual ON lower(TRIM(o.tval_char)) = lower(TRIM(tvqual.src_code))
                                                            AND tvqual.cdm_tbl = 'OBSERVATION_FACT'
                                                            AND tvqual.cdm_tbl_column_name = 'TVAL_CHAR'
        WHERE o.concept_cd like '%UMLS:C1335447%' or o.concept_cd like '%UMLS:C1611271%' 
        or o.concept_cd like '%UMLS:C4303880%' or o.concept_cd like '%UMLS:C1334932%';

    measurementcnt1 := SQL%rowcount;
    COMMIT;

    recordcount := conditioncnt + procedurecnt + measurementcnt + observationcnt + drugcnt + drugcnt1 +measurementcnt1;
    dbms_output.put_line('I2B2ACT observation_fact source data inserted to condition, procedure, measurement, observation, drug staging table, ST_OMOP53_CONDITION_OCCURRENCE, ST_OMOP53_PROCEDURE_OCCURRENCE, and ST_OMOP53_MEASUREMENT, st_omop53_observation, st_omop53_drug_exposure successfully.'
    );
END sp_a2o_src_observation_fact;
