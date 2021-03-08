CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/04 - domain mapping/location` AS

WITH cte_addr as (
    -- Most recent address for each patient
    SELECT * FROM (
        SELECT 
            addressid,
            patid,
            address_city,
            address_state,
            address_zip5,
            address_period_start,
            address_period_end,
            data_partner_id,
            payload,
            Row_Number() Over (Partition By patid 
                Order By address_preferred Desc,                       -- Rank address_preferred = 'Y' over address_preferred = 'N'
                COALESCE(address_period_end, CURRENT_DATE()) Desc,     -- Rank address_preferred = null as highest priority
                address_period_start Desc) as addr_rank
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/lds_address_history` addr_hist
    )
    WHERE addr_rank = 1
),

location as (
    SELECT DISTINCT
        CAST(null as string) as address_1,
        CAST(null as string) as address_2, 
        address_city as city,
        address_state AS state,
        address_zip5 as zip,
        CAST(null as string) as county, 
        CAST(null as string) as location_source_value,
        'LDS_ADDRESS_HISTORY' AS domain_source,
        data_partner_id,
        payload
    FROM cte_addr
)

SELECT DISTINCT
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
        , COALESCE(address_1, '')
        , COALESCE(address_2, '')
        , COALESCE(city, '')
        , COALESCE(state, '')
        , COALESCE(zip, '')
        , COALESCE(county, '')
        , COALESCE(location_source_value, '')
    )) as hashed_id
    FROM location
)