CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/device_exposure` AS

    ---drug2device
    with drug as (
         SELECT
              drug_exposure_id as site_domain_id
            , 'DRUG_EXPOSURE_ID:' || drug_exposure_id as source_pkey
            , person_id as site_person_id
            , d.drug_concept_id as device_concept_id
            , drug_exposure_start_date as device_exposure_start_date
            , drug_exposure_start_datetime as device_exposure_start_datetime
            , drug_exposure_end_date as device_exposure_end_date
            , drug_exposure_end_datetime as device_exposure_end_datetime
            , coalesce(drug_type_concept_id, 32817) as device_type_concept_id
            , CAST(null as string) unique_device_id     
            , CAST(NULL AS INT ) as quantity 
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , drug_source_value as device_source_value
            , drug_source_concept_id as device_source_concept_id
            , 'DRUG' as source_domain
            , data_partner_id
            , payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/drug_exposure` d
    WHERE drug_exposure_id IS NOT NULL and d.domain_id = 'Device' 
    ),

    --procedure2device
    procedure as (
        SELECT
            procedure_occurrence_id as site_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id as source_pkey
            , person_id as site_person_id
            , p.procedure_concept_id as device_concept_id
            , procedure_date as device_exposure_start_date
            , procedure_datetime as device_exposure_start_datetime
            , procedure_date as device_exposure_end_date
            , procedure_datetime as device_exposure_end_datetime
            , coalesce(procedure_type_concept_id, 32817 ) as device_type_concept_id
            , CAST(null as string) unique_device_id     
            , CAST(NULL AS INT ) as quantity 
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value as device_source_value
            , procedure_source_concept_id as device_source_concept_id
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL and p.domain_id = 'Device'
    ),

    --device2device
    device as (
        SELECT d.device_exposure_id as site_domain_id
            , 'DEVICE_EXPOSURE_ID:' || device_exposure_id as source_pkey
            , d.person_id as site_person_id
            , d.device_concept_id
            , d.device_exposure_start_date
            , d.device_exposure_start_datetime
            , d.device_exposure_end_date
            , d.device_exposure_end_datetime
            , coalesce(d.device_type_concept_id, 32817 ) as device_type_concept_id
            , d.unique_device_id     
            , d.quantity 
            , d.provider_id as site_provider_id
            , d.visit_occurrence_id as site_visit_occurrence_id
            , d.visit_detail_id as site_visit_detail_id
            , d.device_source_value
            , d.device_source_concept_id
            , 'DEVICE' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/device_exposure` d
        WHERE d.device_exposure_id IS NOT NULL AND d.domain_id = 'Device'
    ),

    all_domain as (
        select *
         , md5(CAST(source_pkey as string)) as hashed_id
         from (
             select * from drug
             union all 
             select * from procedure
             union all 
             select * from device
         )
    )

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as device_exposure_id_51_bit
    FROM (
        SELECT * from all_domain
    )   