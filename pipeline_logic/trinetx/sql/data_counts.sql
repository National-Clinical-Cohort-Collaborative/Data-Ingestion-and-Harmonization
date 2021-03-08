CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/metadata/data_counts_check_sql` AS
    
WITH parsed_counts AS (
    SELECT 
          'PATIENT' as domain
        , count(*) as parsed_row_count
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/01 - parsed/patient`

    UNION

    SELECT 
          'MEDICATION' as domain
        , count(*) as parsed_row_count
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/01 - parsed/medication`

    UNION

    SELECT 
          'DIAGNOSIS' as domain
        , count(*) as parsed_row_count
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/01 - parsed/diagnosis`

    UNION

    SELECT 
          'VITAL_SIGNS' as domain
        , count(*) as parsed_row_count
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/01 - parsed/vital_signs`

    UNION

    SELECT 
          'PROCEDURE' as domain
        , count(*) as parsed_row_count
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/01 - parsed/procedure`

    UNION 

    SELECT 
          'LAB_RESULT' as domain
        , count(*) as parsed_row_count
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/01 - parsed/lab_result`

    UNION

    SELECT 
          'ENCOUNTER' as domain
        , count(*) as parsed_row_count
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/01 - parsed/encounter`
),

joined_counts AS (
    SELECT 
        parsed_counts.*, 
        cast(data_counts.ROW_COUNT as long) as loaded_row_count
    FROM parsed_counts 
    FULL OUTER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/metadata/site_data_counts` data_counts
    ON parsed_counts.domain = data_counts.TABLE_NAME
)

SELECT 
    joined_counts.*, 
    (loaded_row_count - parsed_row_count) AS delta_row_count 
FROM joined_counts
