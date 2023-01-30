CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/04.5 - deduping/observation_dupes` AS

WITH obs_raw AS (
    SELECT *
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/04.5 - deduping/observation_raw`
),

obs_ids AS (
    SELECT hashed_id
    FROM obs_raw
),

dupes AS (
    SELECT hashed_id, COUNT(hashed_id) as id_count
    FROM obs_ids
    GROUP BY hashed_id
    HAVING id_count > 1
)

SELECT obs_raw.*
FROM obs_raw
INNER JOIN dupes ON dupes.hashed_id = obs_raw.hashed_id
