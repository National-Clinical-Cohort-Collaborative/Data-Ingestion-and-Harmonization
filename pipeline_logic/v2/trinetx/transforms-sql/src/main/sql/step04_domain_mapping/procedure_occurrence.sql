CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/procedure_occurrence` AS

WITH diagnosis_procedure AS (
    SELECT 
        patient_id as site_patient_id,
        cast(date as date) as procedure_date,
        cast(null as timestamp) as procedure_datetime,
        0 as procedure_type_concept_id,
        0 as modifier_concept_id,
        cast(null as int) as quantity,
        cast(null as long) as provider_id,
        encounter_id as site_encounter_id,
        cast(null as long) as visit_detail_id,
        cast(COALESCE(mapped_code, px_code) as string) as procedure_source_value,
        cast(null as string) as modifier_source_value,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(data_partner_id as int) as data_partner_id, 
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/procedure` procedure 

    UNION ALL

    SELECT 
        patient_id as site_patient_id,
        cast(date as date) as procedure_date,
        cast(date as timestamp) as procedure_datetime,
        0 as procedure_type_concept_id,
        0 as modifier_concept_id,
        cast(null as int) as quantity,
        cast(null as long) as provider_id,
        encounter_id as site_encounter_id,
        cast(null as long) as visit_detail_id,
        cast(COALESCE(mapped_code, dx_code) as string) as procedure_source_value,
        cast(null as string) as modifier_source_value,
        PREPARED_COALESCED_MAPPED_CODE_SYSTEM AS mapped_code_system,
        CAST(data_partner_id as int) as data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/diagnosis` diagnosis 
),

crosswalk_lookup AS (
    SELECT
      diagnosis_procedure.*
    , crosswalk.source_concept_id AS procedure_source_concept_id
    , crosswalk.target_concept_id AS procedure_concept_id
    FROM diagnosis_procedure
    INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/t2o_code_xwalk_standard` crosswalk
        ON upper(diagnosis_procedure.mapped_code_system) = upper(crosswalk.src_vocab_code)
        AND diagnosis_procedure.procedure_source_value = crosswalk.source_code
        AND crosswalk.target_domain_id = 'Procedure'
),

final_table AS (
    SELECT
          *
        -- Required for identical rows so that their IDs differ when hashing
        , ROW_NUMBER() OVER (
            PARTITION BY
              site_patient_id
            , procedure_concept_id
            , procedure_date
            , procedure_datetime
            , procedure_type_concept_id
            , modifier_concept_id
            , quantity
            , provider_id
            , site_encounter_id
            , visit_detail_id
            , procedure_source_value
            , procedure_source_concept_id
            , modifier_source_value
            ORDER BY site_patient_id
        ) as row_index
    FROM crosswalk_lookup
    WHERE procedure_concept_id IS NOT NULL
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as procedure_occurrence_id_51_bit
    , hashed_id
    , site_patient_id
    , procedure_concept_id
    , procedure_date
    , procedure_datetime
    , procedure_type_concept_id
    , modifier_concept_id
    , quantity
    , provider_id
    , site_encounter_id
    , visit_detail_id
    , procedure_source_value
    , procedure_source_concept_id
    , modifier_source_value
    , data_partner_id
    , payload
FROM (
    SELECT
          *
        , md5(concat_ws(
              ';'
			, COALESCE(site_patient_id, ' ')
			, COALESCE(procedure_concept_id, ' ')
			, COALESCE(procedure_date, ' ')
			, COALESCE(procedure_datetime, ' ')
			, COALESCE(procedure_type_concept_id, ' ')
			, COALESCE(modifier_concept_id, ' ')
			, COALESCE(quantity, ' ')
			, COALESCE(provider_id, ' ')
			, COALESCE(site_encounter_id, ' ')
			, COALESCE(visit_detail_id, ' ')
			, COALESCE(procedure_source_value, ' ')
			, COALESCE(procedure_source_concept_id, ' ')
			, COALESCE(modifier_source_value, ' ')
			, COALESCE(row_index, ' ')
        )) as hashed_id
    FROM final_table
)
