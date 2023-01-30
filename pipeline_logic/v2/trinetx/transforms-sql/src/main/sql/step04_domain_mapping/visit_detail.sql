CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/visit_detail` AS

  WITH visit_detail_raw AS (
    SELECT *
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/04.5 - deduping/visit_detail_raw`
  ),

  visit_detail_dups AS (
    SELECT hashed_id
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/04.5 - deduping/visit_detail_dups`
  )

  SELECT visit_detail_raw.*
  FROM visit_detail_raw
  LEFT ANTI JOIN visit_detail_dups ON visit_detail_raw.hashed_id = visit_detail_dups.hashed_id
