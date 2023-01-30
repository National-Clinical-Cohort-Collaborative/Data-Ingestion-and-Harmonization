CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/procedure_occurrence` AS
    with procedure as 
    (
           SELECT
              procedure_occurrence_id as site_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id as source_pkey
            , person_id as site_person_id
            , procedure_concept_id
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL AND procedure_concept_id IS NOT NULL
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Observation' OR 'Measurement'...), do not include them in this table
        AND (p.domain_id IS NULL or p.domain_id NOT IN ('Observation', 'Measurement', 'Drug', 'Device'))
     
    ), 

    --measurement2procedures
    measurement as (
        select 
            measurement_id as site_domain_id
            , 'MEASUREMENT_ID:' || measurement_id as source_pkey
            , person_id as site_person_id
            , measurement_concept_id as procedure_concept_id
            , measurement_date as procedure_date
            , measurement_datetime as procedure_datetime
            , measurement_type_concept_id as procedure_type_concept_id
            , value_as_concept_id as modifier_concept_id
            , cast( null as int ) quantity
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , measurement_source_value as procedure_source_value
            , measurement_source_concept_id as procedure_source_concept_id
            , unit_source_value as modifier_source_value
            , 'MEASUREMENT' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/measurement` m
        WHERE measurement_id IS NOT NULL and ( m.domain_id = 'Procedure' ) 
    ), 

    all_domain as (
        select *
         , md5(CAST(source_pkey as string)) as hashed_id
         from (
             select * from procedure
             union all 
             select * from measurement
         )
    )

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as procedure_occurrence_id_51_bit
    FROM (
        SELECT * from all_domain
    )   