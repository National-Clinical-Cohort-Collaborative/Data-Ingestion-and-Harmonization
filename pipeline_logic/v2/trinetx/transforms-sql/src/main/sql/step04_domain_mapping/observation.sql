CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/observation` AS

WITH obs_raw AS (
    SELECT *
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/04.5 - deduping/observation_raw`
),

obs_dupes AS (
    SELECT hashed_id
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/04.5 - deduping/observation_dupes`
)

SELECT obs_raw.*
FROM obs_raw
LEFT ANTI JOIN obs_dupes ON obs_raw.hashed_id = obs_dupes.hashed_id
