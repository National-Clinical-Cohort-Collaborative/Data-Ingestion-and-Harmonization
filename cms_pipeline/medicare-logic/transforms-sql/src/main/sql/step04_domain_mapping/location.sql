CREATE TABLE `ri.foundry.main.dataset.f25302a5-b083-41e5-b5cd-825008e58ebf` AS
    with location as (
    SELECT 
        CAST(pkey as long) as location_id, 
        BID,
        CAST(BID as long) as person_id,
        CAST(null as string) as address_1, 
        CAST(null as string) as address_2, 
        CAST(null as string) as city, 
        CAST(STATE_CODE as string) as state, 
        CAST(ZIP_CD as string) as zip,
        CAST(COUNTY_CD as string) as county, 
        CAST(COUNTY_CD ||'-'||STATE_CODE as string) as location_source_value
    FROM `ri.foundry.main.dataset.a3105849-975a-4c03-b770-26dc7254024d` m
    WHERE m.ZIP_CD IS NOT NULL
    ),
    cms_location as (
    SELECT DISTINCT
        location_id
        , address_1
        , address_2
        , city
        , state
        , zip
        , county
        , location_source_value
    FROM location  
    )
    select DISTINCT * FROM cms_location