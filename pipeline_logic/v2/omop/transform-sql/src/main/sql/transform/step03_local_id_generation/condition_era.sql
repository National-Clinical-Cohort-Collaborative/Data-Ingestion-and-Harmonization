CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/03 - local id generation/condition_era` AS

SELECT 
		* 
	, cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_era_id_51_bit
FROM (
	SELECT
		  condition_era_id as site_condition_era_id
		, md5(CAST(condition_era_id as string)) as hashed_id
		, person_id as site_person_id
		, condition_concept_id
		, condition_era_start_date
		, condition_era_end_date
		, CAST(condition_occurrence_count as int)
		, data_partner_id
		, payload
	FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 777/transform/02 - clean/condition_era`
	WHERE condition_era_id IS NOT NULL
) 
