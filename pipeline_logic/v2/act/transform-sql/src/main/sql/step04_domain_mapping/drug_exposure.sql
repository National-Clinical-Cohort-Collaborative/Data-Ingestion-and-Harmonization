CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/drug_exposure` AS

with obs_fact as (
    SELECT 
        patient_num as site_patient_num,
        CAST(xw.target_concept_id as int) AS drug_concept_id,
        CAST(start_date as date) AS drug_exposure_start_date,
        CAST(start_date as timestamp) AS drug_exposure_start_datetime,
        CAST(end_date as date) AS drug_exposure_end_date,
        CAST(end_date as timestamp) AS drug_exposure_end_datetime,
        CAST(end_date as date) AS verbatim_end_date,
        38000177 AS drug_type_concept_id,
        CAST(null as string) AS stop_reason,
        CAST(null as int) AS refills,
        CAST(ob.quantity_num as float) AS quantity,
        CAST(null AS int) AS days_supply,
        CAST(null AS string) AS sig,
        CAST(null AS int) AS route_concept_id,
        CAST(null AS string) AS lot_number,
        CAST(null AS long) AS provider_id,
        encounter_num as site_encounter_num,
        CAST(null as long) AS visit_detail_id,
        CAST(COALESCE(mapped_concept_cd, concept_cd) as string) AS drug_source_value,
        CAST(xw.source_concept_id as int) AS drug_source_concept_id, --- drug source concept id if it is prescribing
        CAST(ob.modifier_cd as string) AS route_source_value,
        CAST(ob.units_cd as string) AS dose_unit_source_value,
        'OBSERVATION_FACT' AS domain_source,
        site_comparison_key,
        data_partner_id, 
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` ob
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/a2o_code_xwalk` xw 
            ON xw.src_code_type || ':' || xw.src_code = COALESCE(ob.mapped_concept_cd, ob.concept_cd)
            AND xw.cdm_tbl = 'OBSERVATION_FACT'
            AND xw.target_domain_id = 'Drug'
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/a2o_valueset_mapping_table` units 
            ON ob.units_cd = units.SRC_CODE
            AND units.CDM_TBL_COLUMN_NAME = 'UNITS_CD'
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/a2o_valueset_mapping_table` rqual 
            ON lower(TRIM(ob.valueflag_cd)) = lower(TRIM(rqual.SRC_CODE))
            AND rqual.CDM_TBL = 'OBSERVATION_FACT'
            AND rqual.CDM_TBL_COLUMN_NAME = 'VALUEFLAG_CD'

UNION ALL

    SELECT  
        patient_num as site_patient_num,
        37499271 AS drug_concept_id,
        CAST(start_date as date) AS drug_exposure_start_date,
        CAST(start_date as timestamp) AS drug_exposure_start_datetime,
        CAST(end_date as date) AS drug_exposure_end_date,
        CAST(end_date as timestamp) AS drug_exposure_end_datetime,
        CAST(end_date as date) AS verbatim_end_date,
        38000177 AS drug_type_concept_id,
        CAST(null as string) AS stop_reason,
        CAST(null as int) AS refills,
        CAST(ob.quantity_num as float) AS quantity,
        CAST(null AS int) AS days_supply,
        CAST(null AS string) AS sig,
        CAST(null AS int) AS route_concept_id,
        CAST(null AS string) AS lot_number,
        CAST(null AS long) AS provider_id,
        encounter_num as site_encounter_num,
        CAST(null as long) AS visit_detail_id,
        CAST(concept_cd as string) AS drug_source_value,
        37499271 AS drug_source_concept_id, 
        CAST(ob.modifier_cd as string) AS route_source_value,
        CAST(ob.units_cd as string) AS dose_unit_source_value,
        'OBSERVATION_FACT' AS domain_source,
        site_comparison_key,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` ob
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/a2o_valueset_mapping_table` units 
            ON ob.units_cd = units.SRC_CODE
            AND units.CDM_TBL_COLUMN_NAME = 'UNITS_CD'
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/a2o_valueset_mapping_table` rqual 
            ON lower(TRIM(ob.valueflag_cd)) = lower(TRIM(rqual.SRC_CODE))
            AND rqual.CDM_TBL = 'OBSERVATION_FACT'
            AND rqual.CDM_TBL_COLUMN_NAME = 'VALUEFLAG_CD'
        WHERE ob.concept_cd='ACT|LOCAL:REMDESIVIR'
),

final_table AS (
    SELECT
          *
        -- Required for identical rows so that their IDs differ when hashing
        , row_number() OVER (
            PARTITION BY
              site_patient_num
            , drug_concept_id
            , drug_exposure_start_date
            , drug_exposure_start_datetime
            , drug_exposure_end_date
            , drug_exposure_end_datetime
            , verbatim_end_date
            , drug_type_concept_id
            , stop_reason
            , refills
            , quantity
            , days_supply
            , sig
            , route_concept_id
            , lot_number
            , provider_id
            , site_encounter_num
            , visit_detail_id
            , drug_source_value
            , drug_source_concept_id
            , route_source_value
            , dose_unit_source_value   
            ORDER BY site_patient_num        
        ) as row_index
    FROM obs_fact
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_exposure_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patient_num
    , drug_concept_id
    , drug_exposure_start_date
    , drug_exposure_start_datetime
    , drug_exposure_end_date
    , drug_exposure_end_datetime
    , verbatim_end_date
    , drug_type_concept_id
    , stop_reason
    , refills
    , quantity
    , days_supply
    , sig
    , route_concept_id
    , lot_number
    , provider_id
    , site_encounter_num
    , visit_detail_id
    , drug_source_value
    , drug_source_concept_id
    , route_source_value
    , dose_unit_source_value
    , domain_source
    , site_comparison_key
    , data_partner_id
    , payload
    FROM (
        SELECT
          *
        , md5(concat_ws(
              ';'
            , COALESCE(site_patient_num, ' ')
            , COALESCE(drug_concept_id, ' ')
            , COALESCE(drug_exposure_start_date, ' ')
            , COALESCE(drug_exposure_start_datetime, ' ')
            , COALESCE(drug_exposure_end_date, ' ')
            , COALESCE(drug_exposure_end_datetime, ' ')
            , COALESCE(verbatim_end_date, ' ')
            , COALESCE(drug_type_concept_id, ' ')
            , COALESCE(stop_reason, ' ')
            , COALESCE(refills, ' ')
            , COALESCE(quantity, ' ')
            , COALESCE(days_supply, ' ')
            , COALESCE(sig, ' ')
            , COALESCE(route_concept_id, ' ')
            , COALESCE(lot_number, ' ')
            , COALESCE(provider_id, ' ')
            , COALESCE(site_encounter_num, ' ')
            , COALESCE(visit_detail_id, ' ')
            , COALESCE(drug_source_value, ' ')
            , COALESCE(drug_source_concept_id, ' ')
            , COALESCE(route_source_value, ' ')
            , COALESCE(dose_unit_source_value, ' ')
            , COALESCE(row_index, ' ')
            , COALESCE(site_comparison_key, ' ')
        )) as hashed_id
        FROM final_table
    )
