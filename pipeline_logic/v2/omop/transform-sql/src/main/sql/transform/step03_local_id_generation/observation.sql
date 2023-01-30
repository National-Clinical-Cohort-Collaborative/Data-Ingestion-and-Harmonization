CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/observation` AS

    --obs2obs
    with observation as
    (
           SELECT
              observation_id as site_domain_id
            , 'OBSERVATION_ID:' || observation_id as source_pkey
            , person_id as site_person_id
            , observation_concept_id
            , observation_date
            , observation_datetime
            , observation_type_concept_id
            , value_as_number
            , value_as_string
            , value_as_concept_id
            , qualifier_concept_id
            , unit_concept_id
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , observation_source_value
            , observation_source_concept_id
            , unit_source_value
            , qualifier_source_value
            ,'OBSERVATION' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/observation` o
        WHERE observation_id IS NOT NULL
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Measurement' OR 'Condition'), do not include them in this table
        AND (o.domain_id IS NULL or o.domain_id NOT IN ('Measurement', 'Condition'))
    ), 

    --condition2obs
    condition as (
        select 
            condition_occurrence_id as site_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id as source_pkey
            , person_id as site_person_id
            , condition_concept_id as observation_concept_id
            , condition_start_date as observation_date
            , condition_start_datetime as observation_datetime
            , condition_type_concept_id as observation_type_concept_id
            , CAST(null as int ) as value_as_number
            , CAST(null as string ) as value_as_string
            , CAST(null as int ) as value_as_concept_id
            , CAST(null as int ) as qualifier_concept_id
            , CAST(null as int ) as unit_concept_id
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as observation_source_value
            , condition_source_concept_id as observation_source_concept_id
            , CAST(null as string ) as unit_source_value
            , CAST(null as string ) as qualifier_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/condition_occurrence` c
        WHERE condition_occurrence_id IS NOT NULL
        And c.domain_id = 'Observation'

    ), 

    --procedures2observation
    procedures as (
        SELECT 
            procedure_occurrence_id as site_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id as source_pkey
            , person_id as site_person_id
            , procedure_concept_id as observation_concept_id
            , procedure_date as observation_date
            , procedure_datetime as observation_datetime
            , procedure_type_concept_id as observation_type_concept_id
            , CAST(null as float) AS value_as_number
            , CAST(null as string) AS value_as_string
            , CAST(null as int) AS value_as_concept_id
            , CAST(null as int) AS qualifier_concept_id
            , CAST(null as int) AS unit_concept_id
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value as observation_source_value
            , procedure_source_concept_id as observation_source_concept_id
            , CAST( NULL AS string) as unit_source_value
            , CAST( NULL AS string) as qualifier_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        And p.domain_id = 'Observation'
    ),

    all_domain as (
        select
            *
            , md5(CAST(source_pkey as string)) as hashed_id
        from 
        (select * from observation
        union all
        select * from condition
        union all 
        select * from procedures
        )
    )

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_id_51_bit
    FROM (
        SELECT *
        FROM all_domain
    )   