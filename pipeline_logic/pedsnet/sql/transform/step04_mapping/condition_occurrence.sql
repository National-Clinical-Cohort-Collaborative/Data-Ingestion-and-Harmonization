CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/04 - mapping/condition_occurrence` AS

-- condition 2 condition 
with condition as (
    SELECT
          condition_occurrence_id as source_domain_id
        , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
        , person_id as site_person_id
        , COALESCE(c.target_concept_id, 0) as condition_concept_id
        , condition_start_date
        , condition_start_datetime
        , condition_end_date
        , condition_end_datetime
        , condition_type_concept_id
        , condition_status_concept_id
        , stop_reason
        , provider_id as site_provider_id
        , visit_occurrence_id as site_visit_occurrence_id
        , visit_detail_id
        , condition_source_value
        , condition_source_concept_id
        , condition_status_source_value
        , 'CONDITION' as source_domain
        , data_partner_id
        , payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/condition_occurrence` c
    WHERE condition_occurrence_id IS NOT NULL
    AND (c.target_domain_id = 'Condition' OR c.target_domain_id IS NULL)
),

all_domain as (
    select 
        *, 
        md5(CAST(source_pkey as string)) AS hashed_id
    from condition
)

SELECT 
        * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_occurrence_id_51_bit
FROM all_domain
