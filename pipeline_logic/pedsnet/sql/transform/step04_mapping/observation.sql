CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/04 - mapping/observation` AS  

    with observation as (
        SELECT
               observation_id as source_domain_id
             , 'OBSERVATION_ID:' || observation_id || '|TARGET_CONCEPT_ID:' || COALESCE(o.target_concept_id, '') as source_pkey
             , person_id as site_person_id
             , COALESCE(o.target_concept_id, 0) as observation_concept_id
             , observation_date
             , observation_datetime
             , 32817 as observation_type_concept_id
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
             , 'OBSERVATION' as source_domain
             , data_partner_id
             , payload
         FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/observation` o
         WHERE observation_id IS NOT NULL
         AND (o.target_domain_id = 'Observation' OR o.target_domain_id IS NULL)
    ), 

    condition as (      
        SELECT
              condition_occurrence_id as source_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , target_concept_id as observation_concept_id
            , condition_start_date as observation_date
            , condition_start_datetime as observation_datetime
            , 32817 as observation_type_concept_id --ehr origin
            , CAST(null as int ) as value_as_number
            , CAST(null as string) value_as_string
            , condition_status_concept_id as value_as_concept_id -- condition status concept id like final diagnosis
            , CAST(NULL as int) as qualifier_concept_id
            , CAST(NULL as int) as unit_concept_id
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as observation_source_value
            , condition_source_concept_id as observation_source_concept_id
            , CAST( NULL AS string) as unit_source_value
            , CAST( NULL AS string) as qualifier_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/condition_occurrence` c
        WHERE condition_occurrence_id IS NOT NULL
        AND c.target_domain_id = 'Observation'
    ),

    procedures as ( 
        SELECT
            procedure_occurrence_id as source_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(p.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , target_concept_id as observation_concept_id
            , procedure_date as observation_date
            , procedure_datetime as observation_datetime
            , 32817 as observation_type_concept_id
            , CAST(null as int) value_as_number
            , CAST(null as string) value_as_string
            , procedure_concept_id as value_as_concept_id
            , CAST(NULL as int) as qualifier_concept_id
            , CAST(NULL as int) as unit_concept_id
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        AND p.target_domain_id = 'Observation'
    ),

    measurement as (
        SELECT
              measurement_id as source_domain_id
            , 'MEASUREMENT_ID:' || measurement_id || '|TARGET_CONCEPT_ID:' || COALESCE(m.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , m.target_concept_id as observation_concept_id
            , measurement_date as observation_date
            , measurement_datetime as observation_datetime
            --, measurement_time
            , 32817 as observation_type_concept_id
            , m.value_as_number
            , CAST( NULL as string ) as value_as_string
            , m.value_as_concept_id
            , m.operator_concept_id as qualifier_concept_id
            , unit_concept_id
            --, range_low
            --, range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , measurement_source_value as observation_source_value
            , measurement_source_concept_id as observation_source_concept_id
            , m.unit_source_value
            , CAST(NULL AS string) as qualifier_source_value
            , 'MEASUREMENT' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/measurement` m
        WHERE measurement_id IS NOT NULL
        AND m.target_domain_id = 'Observation' 
 
    ),

 all_domain as ( 
        select
            *,
            md5(CAST(source_pkey as string)) AS hashed_id 
        from (
            select * from observation
                union all    
            select * from condition 
                union all 
            select * from procedures
                union all 
            select * from measurement 
        )
    ) 

    SELECT 
        *
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_id_51_bit

    FROM (
             SELECT * FROM all_domain
         )