CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/04 - mapping/procedure_occurrence` AS

-- procedure 2 procedure
    with procedures as (
        SELECT
            procedure_occurrence_id as source_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(p.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(p.target_concept_id, 0) as procedure_concept_id
            , procedure_date
            , procedure_datetime
            , procedure_type_concept_id
            , modifier_concept_id
            , quantity
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value
            , procedure_source_concept_id
            , modifier_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        AND (p.target_domain_id = 'Procedure' OR p.target_domain_id IS NULL)
    ),

    condition as ( 
        SELECT
              condition_occurrence_id as source_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , target_concept_id as procedure_concept_id
            , condition_start_date as procedure_date
            , condition_start_datetime as procedure_datetime
            , 32817 as procedure_type_concept_id
            , CAST(NULL as int) modifier_concept_id
            , CAST(null as int) as quantity
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as procedure_source_value
            , condition_source_concept_id as procedure_source_concept_id
            --, condition_status_source_value
            , CAST( NULL AS string) as modifier_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/condition_occurrence` c
        WHERE condition_occurrence_id IS NOT NULL
        AND c.target_domain_id = 'Procedure'
    ),

    all_domain as ( 
        select
            *,
            md5(CAST(source_pkey as string)) AS hashed_id 
        from (
            select * from procedures 
                union all 
            select * from condition
        )
    ) 

    SELECT 
           * 
         , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as procedure_occurrence_id_51_bit
    FROM (
             SELECT * FROM all_domain
         )
