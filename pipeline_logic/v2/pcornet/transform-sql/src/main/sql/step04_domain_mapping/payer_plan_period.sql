CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/04 - domain mapping/payer_plan_period` AS

with encounter as (
    SELECT 
        site_patid,
        payer_plan_period_start_date,
        payer_plan_period_end_date,
        payer_concept_id,
        payer_source_value,
        payer_source_concept_id,
        plan_concept_id,
        plan_source_value,
        plan_source_concept_id,
        sponsor_concept_id,
        sponsor_source_value,
        sponsor_source_concept_id,
        family_source_value,
        stop_reason_concept_id,
        stop_reason_source_value,
        stop_reason_source_concept_id,
        domain_source,
        data_partner_id,
        payload
    FROM (
        -- primary payers
        SELECT
            patid AS site_patid, 
            CAST(e.admit_date as date) as payer_plan_period_start_date,
            CAST(COALESCE(e.discharge_date, e.admit_date) as date) as payer_plan_period_end_date,
            CAST(xw.TARGET_CONCEPT_ID as int) as payer_concept_id, --get the list of OMOP concept_ids
            'PAYER_TYPE_PRIMARY: ' || e.payer_type_primary as payer_source_value, --this one can stay as the PCORnet source value
            CAST(null as int) as payer_source_concept_id,
            CAST(null as int) as plan_concept_id,
            CAST(null as string) as plan_source_value,
            CAST(null as int) as plan_source_concept_id,
            CAST(null as int) as sponsor_concept_id,
            CAST(null as string) as sponsor_source_value,
            CAST(null as int) as sponsor_source_concept_id,
            CAST(null as string) as family_source_value,
            CAST(null as int) as stop_reason_concept_id,
            CAST(null as string) as stop_reason_source_value,
            CAST(null as int) as stop_reason_source_concept_id,
            'ENCOUNTER' AS domain_source,
            data_partner_id,
            payload,
            ROW_NUMBER() OVER (PARTITION BY 
                e.patid, DATE_FORMAT(admit_date, 'YYYY-MM-DD'), 
                COALESCE(DATE_FORMAT(discharge_date,'YYYY-MM-DD'), DATE_FORMAT(admit_date,'YYYY-MM-DD')), 
                e.payer_type_primary 
                ORDER BY admit_date desc, COALESCE(admit_date, discharge_date) desc, encounterid
                ) as rnk
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/encounter` e
            INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw 
                ON xw.SRC_CODE = e.payer_type_primary
                AND xw.CDM_TBL_COLUMN_NAME = 'PAYER_TYPE_PRIMARY'                   
        WHERE e.payer_type_primary is not null 

        UNION ALL
 
        --secondary payers
        SELECT
            patid AS site_patid, 
            CAST(e.admit_date as date) as payer_plan_period_start_date,
            CAST(COALESCE(e.discharge_date, e.admit_date) as date) as payer_plan_period_end_date,
            CAST(xw.TARGET_CONCEPT_ID as int) as payer_concept_id, --get the list of the OMOP concept_ids
            'PAYER_TYPE_SECONDARY: ' || e.payer_type_secondary as payer_source_value, --this one can stay as the PCORnet source value
            CAST(null as int) as payer_source_concept_id,
            CAST(null as int) as plan_concept_id,
            CAST(null as string) as plan_source_value,
            CAST(null as int) as plan_source_concept_id,
            CAST(null as int) as sponsor_concept_id,
            CAST(null as string) as sponsor_source_value,
            CAST(null as int) as sponsor_source_concept_id,
            CAST(null as string) as family_source_value,
            CAST(null as int) as stop_reason_concept_id,
            CAST(null as string) as stop_reason_source_value,
            CAST(null as int) as stop_reason_source_concept_id,
            'ENCOUNTER' AS domain_source,
            data_partner_id,
            payload,
            ROW_NUMBER() OVER (PARTITION BY 
                e.patid, DATE_FORMAT(admit_date, 'YYYY-MM-DD'), 
                COALESCE(DATE_FORMAT(discharge_date,'YYYY-MM-DD'), DATE_FORMAT(admit_date,'YYYY-MM-DD')), 
                e.payer_type_secondary 
                ORDER BY admit_date desc, COALESCE(admit_date, discharge_date) desc, encounterid
                ) as rnk
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/encounter` e
            INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw 
                ON xw.SRC_CODE = e.payer_type_secondary 
                AND xw.CDM_TBL_COLUMN_NAME = 'PAYER_TYPE_SECONDARY' 
        WHERE e.payer_type_secondary is not null
    ) Payer 
    WHERE rnk=1
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as payer_plan_period_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patid
    , payer_plan_period_start_date
    , payer_plan_period_end_date
    , payer_concept_id
    , payer_source_value
    , payer_source_concept_id
    , plan_concept_id
    , plan_source_value
    , plan_source_concept_id
    , sponsor_concept_id
    , sponsor_source_value
    , sponsor_source_concept_id
    , family_source_value
    , stop_reason_concept_id
    , stop_reason_source_value
    , stop_reason_source_concept_id
    , domain_source
    , data_partner_id
    , payload
FROM (
    SELECT
        *
    , md5(concat_ws(
            ';'
        , COALESCE(site_patid, '')
        , COALESCE(payer_plan_period_start_date, '')
        , COALESCE(payer_plan_period_end_date, '')
        , COALESCE(payer_concept_id, '')
        , COALESCE(payer_source_value, '')
        , COALESCE(payer_source_concept_id, '')
        , COALESCE(plan_concept_id, '')
        , COALESCE(plan_source_value, '')
        , COALESCE(plan_source_concept_id, '')
        , COALESCE(sponsor_concept_id, '')
        , COALESCE(sponsor_source_value, '')
        , COALESCE(sponsor_source_concept_id, '')
        , COALESCE(family_source_value, '')
        , COALESCE(stop_reason_concept_id, '')
        , COALESCE(stop_reason_source_value, '')
        , COALESCE(stop_reason_source_concept_id, '')
    )) as hashed_id
    FROM encounter
)
