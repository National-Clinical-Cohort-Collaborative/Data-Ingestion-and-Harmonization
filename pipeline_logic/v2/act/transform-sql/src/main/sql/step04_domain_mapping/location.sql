CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/location` AS

with location as (
    SELECT DISTINCT
        CAST(null as string) as address_1, 
        CAST(null as string) as address_2, 
        CAST(null as string) as city, 
        CAST(null as string) as state, 
        CAST(zip_cd as string) AS zip,
        CAST(null as string) as county, 
        CAST(null as string) as location_source_value, 
        'PATIENT_DIMENSION' AS domain_source,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/patient_dimension` pd
    WHERE zip_cd IS NOT NULL
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as location_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , address_1
    , address_2
    , city
    , state
    , zip
    , county
    , location_source_value
    , domain_source
    , data_partner_id
    , payload 
FROM (
    SELECT
        *
    , md5(concat_ws(
            ';'
        , COALESCE(address_1, ' ')
        , COALESCE(address_2, ' ')
        , COALESCE(city, ' ')
        , COALESCE(state, ' ')
        , COALESCE(zip, ' ')
        , COALESCE(county, ' ')
        , COALESCE(location_source_value, ' ')
    )) as hashed_id
    FROM location
)
