CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/04 - domain mapping/provider` AS

with provider as (
    SELECT
        CAST(null as string) AS provider_name,
        CAST(null as string) AS npi,
        CAST(null as string) AS dea,
        CAST(null as int) AS specialty_concept_id,
        CAST(null as long) AS care_site_id,
        CAST(null as int) AS year_of_birth,
        CAST(gx.TARGET_CONCEPT_ID as int) AS gender_concept_id,
        CAST(null as string) AS provider_source_value,
        CAST(provider_specialty_primary as string) AS specialty_source_value,
        CAST(null as int) AS specialty_source_concept_id,
        CAST(provider_sex as string) AS gender_source_value,
        CAST(null as int) AS gender_source_concept_id,
        'PROVIDER' AS domain_source,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 777/transform/03 - prepared/provider` p
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/gender_xwalk_table` gx 
            ON gx.CDM_TBL = 'DEMOGRAPHIC'
            AND gx.SRC_GENDER = p.provider_sex
),

final_table as (
    SELECT
          *
        -- Required for identical rows so that their IDs differ when hashing
        , row_number() OVER (
            PARTITION BY
                  provider_name
                , npi
                , dea
                , specialty_concept_id
                , care_site_id 
                , year_of_birth
                , gender_concept_id
                , provider_source_value
                , specialty_source_value
                , specialty_source_concept_id
                , gender_source_value
                , gender_source_concept_id
            ORDER BY specialty_source_value        
        ) as row_index
    FROM provider 
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as provider_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , provider_name
    , npi
    , dea
    , specialty_concept_id
    , care_site_id 
    , year_of_birth
    , gender_concept_id
    , provider_source_value
    , specialty_source_value
    , specialty_source_concept_id
    , gender_source_value
    , gender_source_concept_id
    , domain_source
    , data_partner_id
    , payload
FROM (
    SELECT
        *
    , md5(concat_ws(
            ';'
        , COALESCE(provider_name, '')
        , COALESCE(npi, '')
        , COALESCE(dea, '')
        , COALESCE(specialty_concept_id, '')
        , COALESCE(care_site_id, '')
        , COALESCE(year_of_birth, '')
        , COALESCE(gender_concept_id, '')
        , COALESCE(provider_source_value, '')
        , COALESCE(specialty_source_value, '')
        , COALESCE(specialty_source_concept_id, '')
        , COALESCE(gender_source_value, '')
        , COALESCE(gender_source_concept_id, '')
        , COALESCE(row_index, '')
    )) as hashed_id
    FROM final_table
)