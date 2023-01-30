CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/condition_occurrence` AS
    
    --condition2condition
    with condition as (
        SELECT
            condition_occurrence_id as site_domain_id
            ----SSH Note: if we are joining relationship table to grab the the full target concepts columns, 
            --- then there may be instances where the non-standard code can be mapped to multiple target_concept_id
            ----for those instances, we would need to create unique set of source_pkey. And use the source_pkey to generate the hashed_id
            --  , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id as source_pkey
            , person_id as site_person_id
            , condition_concept_id
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/condition_occurrence` c
        --domain id can be in Condition NULL or Observation
        WHERE c.condition_occurrence_id IS NOT NULL
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Observation'), do not include them in this table
        AND (c.domain_id IS NULL or c.domain_id NOT IN ('Observation'))
    ), 
    --observation2condition
    observation as (
        select
            observation_id as site_domain_id
             , 'OBSERVATION_ID:' || observation_id as source_pkey 
             , person_id as site_person_id
             , observation_concept_id as condition_concept_id
             , observation_date as condition_start_date
             , observation_datetime as condition_start_datetime
             , cast(NULL as date) as condition_end_date
             , cast(Null as timestamp) as condition_end_datetime
             , observation_type_concept_id as condition_type_concept_id
             , cast( null as int) as condition_status_concept_id
             , cast( null as string ) as stop_reason
             , provider_id as site_provider_id
             , visit_occurrence_id as site_visit_occurrence_id
             , visit_detail_id
             , observation_source_value as condition_source_value
             , observation_source_concept_id as condition_source_concept_id
             , cast( null as string ) as condition_status_source_value
             , 'OBSERVATION' as source_domain
             , data_partner_id
             , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/observation` o
        WHERE o.observation_id IS NOT NULL
        and o.domain_id = 'Condition'
    ),
    
    all_domain as (
        select 
        *, 
        md5(CAST(source_pkey as string)) as hashed_id
        from (
            select * from condition
            union all
            select * from observation
            )
    )

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_occurrence_id_51_bit
    FROM  all_domain 