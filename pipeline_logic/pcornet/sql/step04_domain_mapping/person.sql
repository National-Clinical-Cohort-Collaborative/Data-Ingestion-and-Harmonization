CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/04 - domain mapping/person` AS

with patients_with_records AS (
    SELECT DISTINCT patid FROM (
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/condition`		
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/death`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/death_cause`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/diagnosis`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/dispensing`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/encounter`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/immunization`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/lab_result_cm`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/med_admin`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/obs_clin`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/obs_gen`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/prescribing`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/pro_cm`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/procedures`
            UNION ALL
        SELECT DISTINCT patid FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/vital`
    )
),

demographic as (
    SELECT 
        demo.patid AS site_patid,
        cast(gx.TARGET_CONCEPT_ID as int) AS gender_concept_id,
        YEAR(birth_date) AS year_of_birth,
        MONTH(birth_date) AS month_of_birth,
        1 AS day_of_birth,
        cast(null as timestamp) as birth_datetime, --** MB: could use derived 'BIRTH_DATETIME' column, but times seem to all be 00:00
        -- rx.TARGET_CONCEPT_ID AS race_concept_id, 
        CASE 
            WHEN demo.race != '06' OR (demo.race = '06' AND demo.raw_race is null) 
                THEN cast(rx.TARGET_CONCEPT_ID as int)
            ELSE cast(null as int)
        END AS race_concept_id,
        cast(ex.TARGET_CONCEPT_ID as int) AS ethnicity_concept_id, 
        -- lds.N3cds_Domain_Map_Id AS LOCATIONID, --** MB: this gets brought in through a join in step 6
        cast(null as int) as provider_id,
        cast(null as int) as care_site_id,
        cast(demo.patid as string) AS person_source_value, 
        cast(demo.sex as string) AS gender_source_value,  
        0 as gender_source_concept_id, 
        cast(demo.race as string) AS race_source_value, 
        0 AS race_source_concept_id,  
        cast(demo.hispanic as string) AS ethnicity_source_value, 
        0 AS ethnicity_source_concept_id, 
        'DEMOGRAPHIC' AS domain_source,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/demographic` demo  
        --** Remove patients with no records:
        INNER JOIN patients_with_records ON demo.patid = patients_with_records.patid
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/gender_xwalk_table` gx 
            ON gx.CDM_TBL = 'DEMOGRAPHIC'
            AND demo.sex = gx.SRC_GENDER 
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/ethnicity_xwalk_table` ex 
            ON ex.CDM_TBL = 'DEMOGRAPHIC' 
            AND demo.hispanic = ex.SRC_ETHNICITY 
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/race_xwalk_table` rx 
            ON rx.CDM_TBL = 'DEMOGRAPHIC' 
            AND demo.race = rx.SRC_RACE 
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
                , md5(site_patid) as hashed_id
                FROM demographic
            )
        )
    )