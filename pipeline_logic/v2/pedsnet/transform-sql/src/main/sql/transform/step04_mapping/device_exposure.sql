CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 777/transform/04 - mapping/device_exposure` AS

    with condition as 
    (
        SELECT 
            condition_occurrence_id as source_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(c.target_concept_id, 0) as device_concept_id 
            , condition_start_date as device_exposure_start_date
            , condition_start_datetime as device_exposure_start_datetime
            , condition_end_date as device_exposure_end_date
            , condition_end_datetime as device_exposure_end_datetime
            , 32817 as device_type_concept_id
            , CAST(null as string) unique_device_id     
            --, condition_status_concept_id
            --, stop_reason
            , CAST(NULL AS INT ) as quantity 
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as device_source_value
            , condition_source_concept_id as device_source_concept_id
            --, condition_status_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
            FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 777/transform/03 - prepared/condition_occurrence` c
            WHERE condition_occurrence_id IS NOT NULL
            AND c.target_domain_id = 'Device'

    ),

    procedures as (
          SELECT
            procedure_occurrence_id as source_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(p.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , p.target_concept_id as device_concept_id
            , procedure_date as device_exposure_start_date
            , procedure_datetime as device_exposure_start_datetime
            , procedure_date as device_exposure_end_date
            , procedure_datetime as device_exposure_end_datetime
            , 32817 as device_type_concept_id
            , CAST(null as string) unique_device_id     
            , CAST(NULL AS INT ) as quantity 
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value as device_source_value
            , procedure_source_concept_id as device_source_concept_id
            --, modifier_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 777/transform/03 - prepared/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        AND p.target_domain_id = 'Device'

    ),

    all_domain as (
        select 
            *, 
            md5(CAST(source_pkey as string)) AS hashed_id
        from (
            select * from condition 
                union all  
            select * from procedures
        ) 
    )

SELECT 
        * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as device_exposure_id_51_bit
FROM all_domain