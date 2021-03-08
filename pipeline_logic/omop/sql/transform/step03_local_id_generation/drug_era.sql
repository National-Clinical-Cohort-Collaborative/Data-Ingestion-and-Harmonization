CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/03 - local id generation/drug_era` AS

SELECT 
		* 
	, cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_era_id_51_bit
FROM (
	SELECT
		  drug_era_id as site_drug_era_id
		, md5(CAST(drug_era_id as string)) as hashed_id
		, person_id as site_person_id
		, drug_concept_id
		, drug_era_start_date
		, drug_era_end_date
		, CAST(drug_exposure_count as int)
		, CAST(gap_days as int)
		, data_partner_id
		, payload
	FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/02 - clean/drug_era`
	WHERE drug_era_id IS NOT NULL
) 
