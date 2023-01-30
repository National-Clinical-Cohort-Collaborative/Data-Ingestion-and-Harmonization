CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/person` AS

with patients_with_records AS (
    SELECT DISTINCT patient_num FROM (
        SELECT DISTINCT patient_num FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/visit_dimension`		
            UNION ALL
        SELECT DISTINCT patient_num FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact`
    )
),

pat_dim as (
    SELECT
        pd.patient_num AS site_patient_num,
        CAST(COALESCE(gender_xwalk.TARGET_CONCEPT_ID, 0) as int) AS gender_concept_id,
        YEAR(birth_date) AS year_of_birth,
        MONTH(birth_date) AS month_of_birth,
        DAYOFMONTH(birth_date) AS day_of_birth, 
        -- 1 AS day_of_birth,
        CAST(null as timestamp) AS birth_datetime,
        CAST(COALESCE(race_xwalk.TARGET_CONCEPT_ID, 0) as int) AS race_concept_id, 
        --CASE WHEN pd.RACE != '06' OR (demo.RACE='06' AND demo.raw_race is null) then race_xwalk.TARGET_CONCEPT_ID
        --    ELSE null
        --    END AS race_concept_id,
        CAST(COALESCE(ethnicity_xwalk.TARGET_CONCEPT_ID, 0) as int) AS ethnicity_concept_id,
        loc.hashed_id as location_hashed_id, --** Use later to join on location table for global id
        CAST(null as long) as provider_id,
        CAST(null as long) as care_site_id,
        CAST(pd.patient_num as string) AS person_source_value,
        CAST(COALESCE(pd.sex_cd, sex.concept_cd) as string) AS gender_source_value,
        0 AS gender_source_concept_id,
        CAST(COALESCE(pd.race_cd, race.concept_cd) as string) AS race_source_value,
        0 AS race_source_concept_id,
        CAST(COALESCE(pd.ethnicity_cd, ethnicity.concept_cd) as string) AS ethnicity_source_value,
        0 AS ethnicity_source_concept_id,
        'PATIENT_DIMENSION' AS domain_source,
        pd.data_partner_id,
        pd.payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/patient_dimension` pd
        --** Remove patients with no records:
        INNER JOIN patients_with_records ON pd.patient_num = patients_with_records.patient_num
        LEFT JOIN (
                    select patient_num, concept_cd, ROW_NUMBER() OVER (PARTITION BY patient_num ORDER BY start_date DESC) row_num
                        from `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` ethnicity_ob 
                        where upper(concept_cd) like 'DEM|HISP%'
                    ) ethnicity 
                    ON ethnicity.patient_num = pd.patient_num 
                    and ethnicity.row_num = 1
        LEFT JOIN (
                    select patient_num, concept_cd, ROW_NUMBER() OVER (PARTITION BY patient_num ORDER BY start_date DESC) row_num
                    from `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` race_ob 
                    where upper(concept_cd) like 'DEM|RACE%'
                    ) race 
                    ON race.patient_num = pd.patient_num 
                    and race.row_num = 1
        LEFT JOIN (
                    select patient_num, concept_cd, ROW_NUMBER() OVER ( PARTITION BY patient_num ORDER BY start_date DESC) row_num
                    from `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact` sex_ob 
                    where upper(concept_cd) like 'DEM|SEX%'
                    ) sex 
                    ON sex.patient_num = pd.patient_num 
                    and sex.row_num = 1
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/gender_xwalk_table` gender_xwalk 
            ON gender_xwalk.CDM_NAME = 'I2B2ACT'
            AND gender_xwalk.CDM_TBL = 'PATIENT_DIMENSION'
            AND gender_xwalk.SRC_GENDER = COALESCE(pd.sex_cd, sex.concept_cd)
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/ethnicity_xwalk_table` ethnicity_xwalk 
            ON ethnicity_xwalk.CDM_NAME = 'I2B2ACT'
            AND ethnicity_xwalk.CDM_TBL = 'PATIENT_DIMENSION'
            AND ethnicity_xwalk.SRC_ETHNICITY = COALESCE(pd.ethnicity_cd, ethnicity.concept_cd)
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/race_xwalk_table` race_xwalk 
            ON race_xwalk.CDM_NAME = 'I2B2ACT'
            AND race_xwalk.CDM_TBL = 'PATIENT_DIMENSION'
            AND race_xwalk.SRC_RACE = COALESCE(pd.race_cd, race.concept_cd)
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/location` loc 
            ON loc.zip = pd.zip_cd
)

SELECT
      *
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    , cast(base_10_hash_value as bigint) & 2251799813685247 as person_id_51_bit
    FROM (
        SELECT
          *
        , conv(sub_hash_value, 16, 10) as base_10_hash_value
        FROM (
            SELECT
              *
            , substr(hashed_id, 1, 15) as sub_hash_value
            FROM (
                SELECT
                  *
                -- Create primary key by hashing patient id to 128bit hexademical with md5,
                -- and converting to 51 bit int by first taking first 15 hexademical digits and converting
                --  to base 10 (60 bit) and then bit masking to extract the first 51 bits
                , md5(CAST(site_patient_num as string)) as hashed_id
                FROM pat_dim
            )
        )
    )