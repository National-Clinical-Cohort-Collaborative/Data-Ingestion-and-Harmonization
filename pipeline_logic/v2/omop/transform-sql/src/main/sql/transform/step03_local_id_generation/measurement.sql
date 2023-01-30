CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/measurement` AS

    --measurement2measurement
    with measurement as (
         SELECT
              measurement_id as site_domain_id
            , 'MEASUREMENT_ID:' || measurement_id as source_pkey
            , person_id as site_person_id
            , measurement_concept_id
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/measurement` m
        WHERE measurement_id IS NOT NULL
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Procedure'), do not include them in this table
        AND (m.domain_id IS NULL or m.domain_id NOT IN ('Procedure'))

    ), 

    --procedures2measurement
    procedures as (
        SELECT
            procedure_occurrence_id as site_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id as source_pkey
            , person_id as site_person_id
            , procedure_concept_id as measurement_concept_id
            , procedure_date as measurement_date
            , CAST(procedure_datetime as timestamp) as measurement_datetime 
            , CAST( NULL AS string ) as measurement_time
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
            , procedure_source_value as value_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        AND p.domain_id = 'Measurement'
    ),
    
    --observation2measurement
    observation as (
        SELECT
            observation_id as site_domain_id
            , 'OBSERVATION_ID:' || observation_id as source_pkey
            , person_id as site_person_id
            , o.observation_concept_id as measurement_concept_id
            , o.observation_date as measurement_date
            , CAST(o.observation_datetime as timestamp) as measurement_datetime
            , CAST(null as string) AS measurement_time
            , observation_type_concept_id as measurement_type_concept_id
            , cast(null as int) as operator_concept_id
            , value_as_number
            , value_as_concept_id
            , unit_concept_id
            , CAST( NULL as float ) as range_low
            , CAST( NULL as float ) as range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , observation_source_value as measurement_source_value
            , observation_source_concept_id as measurement_source_concept_id
            , unit_source_value
            , CAST(null as string) as value_source_value
            , 'OBSERVATION' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/observation` o
        WHERE observation_id IS NOT NULL
        AND o.domain_id = 'Measurement'
    ),

    all_domain as ( 
        select
            *,
            md5(CAST(source_pkey as string)) AS hashed_id 
        from (
            select * from measurement 
            union all 
            select * from procedures
            union all
            select * from observation
        )
    ) 

    SELECT 
            * 
            , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as measurement_id_51_bit
    FROM (
        select * from all_domain
         )   