CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/person` AS

with persons_to_keep AS (
    SELECT DISTINCT patient_id FROM (
        SELECT DISTINCT patient_id FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/diagnosis`
            UNION ALL
        SELECT DISTINCT patient_id FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/encounter`
            UNION ALL
        SELECT DISTINCT patient_id FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/lab_result`
            UNION ALL
        SELECT DISTINCT patient_id FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/medication`
            UNION ALL
        SELECT DISTINCT patient_id FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/procedure`
            UNION ALL
        SELECT DISTINCT patient_id FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/vital_signs`
    )	
),

person as (
    SELECT 
        patient_id as site_patient_id,
        CAST(COALESCE(mapped_sex_concept_id, 0) as int) as gender_concept_id,
        CAST(SUBSTRING(birth_date, 1, 4) AS int) as year_of_birth,
        CAST(SUBSTRING(birth_date, 6, 2) AS int) as month_of_birth,
        CAST(null as int) as day_of_birth,
        CAST(null as timestamp) as birth_datetime,
        CAST(COALESCE(mapped_race_concept_id, 0) as int) as race_concept_id,
        CAST(COALESCE(mapped_ethnicity_concept_id, 0) as int) as ethnicity_concept_id,
        CAST(null as long) as provider_id,
        CAST(null as long) as care_site_id,
        CAST(patient_id as string) as person_source_value,
        CAST(COALESCE(mapped_sex, sex) as string) as gender_source_value,
        CAST(null as int) as gender_source_concept_id,
        CAST(COALESCE(mapped_race, race) as string) as race_source_value,
        CAST(null as int) as race_source_concept_id,
        CAST(COALESCE(mapped_ethnicity, ethnicity) as string) as ethnicity_source_value,
        CAST(null as int) as ethnicity_source_concept_id,
        CAST(data_partner_id as int) as data_partner_id,
        payload,
        postal_code
    FROM (
        SELECT patient.* 
        , id_mapping_race.TARGET_CONCEPT_ID as mapped_race_concept_id
        , id_mapping_sex.TARGET_CONCEPT_ID as mapped_sex_concept_id
        , id_mapping_ethnicity.TARGET_CONCEPT_ID as mapped_ethnicity_concept_id
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/03 - prepared/patient` patient 
            -- Drop patients with no records		
            INNER JOIN persons_to_keep		
                ON patient.patient_id = persons_to_keep.patient_id
            LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/t2o_valueset_mapping_table` id_mapping_race
                ON id_mapping_race.CDM_TBL = 'patient'
                AND id_mapping_race.CDM_TBL_COLUMN_NAME = 'mapped_race'
                AND patient.mapped_race = id_mapping_race.SRC_CODE
            LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/t2o_valueset_mapping_table` id_mapping_sex
                ON id_mapping_sex.CDM_TBL = 'patient'
                AND id_mapping_sex.CDM_TBL_COLUMN_NAME = 'mapped_sex'
                AND patient.mapped_sex = id_mapping_sex.SRC_CODE
            LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/t2o_valueset_mapping_table` id_mapping_ethnicity
                ON id_mapping_ethnicity.CDM_TBL = 'patient'
                AND id_mapping_ethnicity.CDM_TBL_COLUMN_NAME = 'mapped_ethnicity'
                AND patient.mapped_ethnicity = id_mapping_ethnicity.SRC_CODE
    )
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
                , md5(site_patient_id) as hashed_id
                FROM person
            )
        )
    )