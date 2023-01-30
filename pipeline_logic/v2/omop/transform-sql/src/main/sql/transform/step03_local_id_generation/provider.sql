CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/provider` AS

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as provider_id_51_bit
    FROM (
        SELECT
              provider_id as site_provider_id
            , md5(CAST(provider_id as string)) as hashed_id
            , provider_name
            , npi
            , dea
            , specialty_concept_id
            , care_site_id as site_care_site_id
            , year_of_birth
            , gender_concept_id
            , provider_source_value
            , specialty_source_value
            , specialty_source_concept_id
            , gender_source_value
            , gender_source_concept_id
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/provider`
        WHERE provider_id IS NOT NULL
    )   