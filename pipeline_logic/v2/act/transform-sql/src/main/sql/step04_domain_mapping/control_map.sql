CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/control_map` AS
    SELECT 
        * 
        , CAST(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as control_map_id_51_bit
    FROM (
        SELECT
            control_map_id as site_control_map_id
            , md5(CAST(control_map_id AS STRING)) as hashed_id
            , case_person_id as site_case_person_id
            , buddy_num
            , control_person_id as site_control_person_id
            , CAST(NULL as INTEGER) as case_age	
            , CAST(NULL as STRING) as case_sex	
            , CAST(NULL as STRING) as case_race	
            , CAST(NULL as STRING) as case_ethn	
            , CAST(NULL as INTEGER) as control_age	
            , CAST(NULL as STRING) as control_sex	
            , CAST(NULL as STRING) as control_race	
            , CAST(NULL as STRING) as control_ethn
            , data_partner_id
            , payload
        FROM ( 
            SELECT
                CAST( (case_patid || buddy_num || control_patid) AS STRING) as control_map_id ,
                case_patid as case_person_id,
                buddy_num, 
                control_patid as control_person_id, 
                data_partner_id,
                payload
                FROM (
                    select distinct case_patid, buddy_num, control_patid, data_partner_id, payload
                    from 
                    `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/control_map` cm
                    WHERE case_patid IS NOT NULL and buddy_num is not null and control_patid is not null 
            ) 
        )
    )        