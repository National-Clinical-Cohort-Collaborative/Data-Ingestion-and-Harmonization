CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/drug_exposure` AS

    with drugexp as ( 
        SELECT
              drug_exposure_id as site_domain_id
            , 'DRUG_EXPOSURE_ID:' || drug_exposure_id as source_pkey
            , person_id as site_person_id
            , drug_concept_id 
            , drug_exposure_start_date
            , drug_exposure_start_datetime
            , drug_exposure_end_date
            , drug_exposure_end_datetime
            , verbatim_end_date
            , drug_type_concept_id
            , stop_reason
            , refills
            , quantity
            , days_supply
            , sig
            , route_concept_id
            , lot_number
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , drug_source_value
            , drug_source_concept_id
            , route_source_value
            , dose_unit_source_value
            , 'DRUG' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/drug_exposure` d
        -- site are sending drug that may contain drug/null/device
        WHERE drug_exposure_id IS NOT NULL
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Device'), do not include them in this table
        AND (d.domain_id IS NULL or d.domain_id NOT IN ('Device'))
    ),

    ---proc2drug
    procedures as (
        SELECT
            procedure_occurrence_id as site_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id as source_pkey
            , person_id as site_person_id
            , procedure_concept_id as drug_concept_id
            , procedure_date as drug_exposure_start_date
            , procedure_datetime as drug_exposure_start_datetime
            , procedure_date as drug_exposure_end_date
            , procedure_datetime as drug_exposure_end_datetime
            , CAST(NULL as date) verbatim_end_date
            , 32817 as drug_type_concept_id
            , CAST(NULL AS string ) as stop_reason
            , CAST(NULL AS int ) as refills
            , CAST( quantity AS float) as quantity
            , CAST(NULL AS int ) as days_supply
            , CAST(NULL AS string ) as sig
            , case 
                when procedure_source_value like '%INJECT%' THEN 4312507
                when procedure_source_value like '%FLU VACC%' OR procedure_source_value like '%POLIOVIRUS%' OR procedure_source_value like '%VACCINE%' THEN 4295880
                else CAST( null as int ) 
                END as route_concept_id
            , CAST(NULL AS string ) as lot_number
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value as drug_source_value
            , procedure_source_concept_id as drug_source_concept_id
            , procedure_source_value as route_source_value
            , procedure_source_value as dose_unit_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        AND p.domain_id = 'Drug'
    ),

  all_domain as ( 
        select 
            *,
            md5(CAST(source_pkey as string)) AS hashed_id 
        from (
            select * from drugexp 
                union all  
            select * from procedures
        )
    ) 

    SELECT 
        * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_exposure_id_51_bit
    FROM (
        select * from all_domain
    )