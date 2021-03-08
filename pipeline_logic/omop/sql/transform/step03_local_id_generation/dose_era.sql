CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/03 - local id generation/dose_era` AS

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as dose_era_id_51_bit
    FROM (
        SELECT
              dose_era_id as site_dose_era_id
            , md5(CAST(dose_era_id as string)) as hashed_id
            , person_id	as site_person_id
            , drug_concept_id	
            , unit_concept_id	
            , dose_value	
            , dose_era_start_date	
            , dose_era_end_date	
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/02 - clean/dose_era`
        WHERE dose_era_id IS NOT NULL
    )   
