CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/04 - mapping/measurement` AS

    --measurement2measurement
    with measurement as (
        SELECT
              measurement_id as source_domain_id
            , 'MEASUREMENT_ID:' || measurement_id || '|TARGET_CONCEPT_ID:' || COALESCE(m.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(m.target_concept_id, 0) as measurement_concept_id
            , measurement_date
            , measurement_datetime
            , measurement_time
            , measurement_type_concept_id
            , operator_concept_id
            , value_as_number
            , value_as_concept_id
            , unit_concept_id
            , range_low
            , range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , measurement_source_value
            , measurement_source_concept_id
            , unit_source_value
            , value_source_value
            , 'MEASUREMENT' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/measurement` m
        WHERE measurement_id IS NOT NULL
        AND (m.target_domain_id = 'Measurement' OR m.target_domain_id IS NULL)
    ),

    --condition2measurement
    condition as (
        SELECT
              condition_occurrence_id as source_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , c.target_concept_id as measurement_concept_id
            , condition_start_date as measurement_date
            , condition_start_datetime as measurement_datetime
            , CAST( NULL AS string ) as measurement_time
            , 32817 as measurement_type_concept_id
            , CAST( NULL AS int) as operator_concept_id
            , CAST( NULL AS int) as value_as_number
            , COALESCE(cr.concept_id_2, condition_concept_id) as value_as_concept_id 
            , CAST( NULL AS int) as unit_concept_id
            , null as range_low
            , null as range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as measurement_source_value
            , condition_source_concept_id as measurement_source_concept_id
            , cast( null as string) as unit_source_value
            , condition_status_source_value as value_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/condition_occurrence` c
        -- Map non-standard measurement value_as_concept_ids to standard when possible
        LEFT JOIN `/UNITE/OMOP Vocabularies/concept_relationship` cr
            ON cr.relationship_id = 'Maps to value' 
            AND c.condition_concept_id = cr.concept_id_1
        WHERE condition_occurrence_id IS NOT NULL
        AND c.target_domain_id = 'Measurement'
    ), 

    --procedures2measurement
    procedures as (
        SELECT
            procedure_occurrence_id as source_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(p.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , p.target_concept_id as measurement_concept_id
            , procedure_date as measurement_date
            , CAST(procedure_datetime as timestamp) as measurement_datetime
            --, CAST( null as timestamp) as measurement_datetime
            , CAST(null as string) AS measurement_time
            , 32817 as measurement_type_concept_id
            , modifier_concept_id as operator_concept_id
            , CAST( NULL as float ) as value_as_number
            , CAST( NULL as int ) as value_as_concept_id
            , CAST( NULL as int ) as unit_concept_id
            , CAST( NULL as float ) as range_low
            , CAST( NULL as float ) as range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value as measurement_source_value
            , procedure_source_concept_id as measurement_source_concept_id
            , modifier_source_value as unit_source_value
            , p.source_concept_name as value_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        AND p.target_domain_id = 'Measurement'

    ),

    --measurement-metadata-- no target_concept_id are present for target_domain_id is null and source_domain_id = Metadata
    --meas
    measvalue as (
        SELECT
              condition_occurrence_id as source_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            -- 11/12/20 - SH: c.condition_source_concept_id contains non-standard code/ target_concept_id contains the qualitative result value like 9190
            -- We will need to update to use the target_concept_id when it is corrected from the prepare step. 
            ---11/12/20-SH: target_concept_id contains the qualitative result / not the concept id values. / using condition_source_concept_id for now.
            --- may need to revisit Meas Value from Condition to Measurement/ as the target domain map may change. 
            ---replace with the c.target_concept_id as measurement_concept_id
            , c.condition_source_concept_id as measurement_concept_id 
            , condition_start_date as measurement_date
            , condition_start_datetime as measurement_datetime
            , CAST( NULL AS string ) as measurement_time
            , 32817 as measurement_type_concept_id
            , CAST( NULL AS int) as operator_concept_id
            , CAST( NULL AS int) as value_as_number
            ,condition_concept_id as value_as_concept_id 
            , CAST( NULL AS int) as unit_concept_id
            , null as range_low
            , null as range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as measurement_source_value
            , condition_source_concept_id as measurement_source_concept_id
            , cast( null as string) as unit_source_value
            , condition_status_source_value as value_source_value
            , 'CONDITION-MEAS_VALUE' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/condition_occurrence` c
        WHERE condition_occurrence_id IS NOT NULL
        AND c.target_domain_id = 'Meas Value'
    ), 

    all_domain as ( 
        select
            *,
            md5(CAST(source_pkey as string)) AS hashed_id 
        from (
            select * from measurement 
            union all 
            select * from condition
            union all 
            select * from procedures
            union all 
            select * from measvalue
        )
    ) 

    SELECT 
           * 
         , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as measurement_id_51_bit
    FROM (
             SELECT * FROM all_domain
         )