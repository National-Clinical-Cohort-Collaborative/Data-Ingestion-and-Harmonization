CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/condition_era` AS

-- This script is taken from here:
-- https://github.com/OHDSI/ETL-CMS/blob/master/SQL/create_CDMv5_condition_era.sql

WITH cteConditionTarget AS
(
	SELECT
		co.hashed_id as condition_occurrence_hashed_id
		, co.site_patient_num as person_id -- refer to site_patient_num as person_id for intermediate steps, then rename in final SELECT statement
		, co.condition_concept_id
		, co.condition_start_date
		, COALESCE(NULLIF(co.condition_end_date,NULL), date_add(condition_start_date, 1)) AS condition_end_date
		, data_partner_id
		, payload
	FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/04 - domain mapping/condition_occurrence` co
	/* Depending on the needs of your data, you can put more filters on to your code. We assign 0 to our unmapped condition_concept_id's,
	 * and since we don't want different conditions put in the same era, we put in the filter below.
 	 */
	WHERE condition_concept_id != 0
),
--------------------------------------------------------------------------------------------------------------
cteEndDates AS -- the magic
(
	SELECT
		person_id
		, condition_concept_id
		, date_add(event_date,  -30) AS end_date -- unpad the end date
	FROM
	(
		SELECT
			person_id
			, condition_concept_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal -- this pulls the current START down from the prior rows so that the NULLs from the END DATES will contain a value we can compare with
			, ROW_NUMBER() OVER (PARTITION BY person_id, condition_concept_id ORDER BY event_date, event_type) AS overall_ord -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
		FROM
		(
			-- select the start dates, assigning a row number to each
			SELECT
				person_id
				, condition_concept_id
				, condition_start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id
				, condition_concept_id ORDER BY condition_start_date) AS start_ordinal
			FROM cteConditionTarget

			UNION ALL

			-- pad the end dates by 30 to allow a grace period for overlapping ranges.
			SELECT
				person_id
			    , condition_concept_id
				, date_add(condition_end_date, 30)
				, 1 AS event_type
				, NULL
			FROM cteConditionTarget
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
--------------------------------------------------------------------------------------------------------------
cteConditionEnds AS
(
SELECT
      c.person_id
	, c.condition_concept_id
	, c.condition_start_date
	, MIN(e.end_date) AS era_end_date
	, data_partner_id
	, payload
FROM cteConditionTarget c
JOIN cteEndDates e ON c.person_id = e.person_id AND c.condition_concept_id = e.condition_concept_id AND e.end_date >= c.condition_start_date
GROUP BY
      c.condition_occurrence_hashed_id
	, c.person_id
	, c.condition_concept_id
	, c.condition_start_date
	, data_partner_id
	, payload
),
--------------------------------------------------------------------------------------------------------------
final_table AS
(
SELECT
	person_id as site_patient_num
	, condition_concept_id
	, MIN(condition_start_date) AS condition_era_start_date
	, era_end_date AS condition_era_end_date
	, COUNT(*) AS condition_occurrence_count
	, data_partner_id
	, payload
FROM cteConditionEnds
GROUP BY person_id, condition_concept_id, era_end_date, data_partner_id, payload
ORDER BY person_id, condition_concept_id
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_era_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
	, site_patient_num
	, condition_concept_id
	, condition_era_start_date
	, condition_era_end_date
	, CAST(condition_occurrence_count as int)
    , data_partner_id
	, payload
	FROM (
        SELECT
        *
        , md5(concat_ws(
              ';'
			, COALESCE(site_patient_num, ' ')
			, COALESCE(condition_concept_id, ' ')
			, COALESCE(condition_era_start_date, ' ')
			, COALESCE(condition_era_end_date, ' ')
			, COALESCE(condition_occurrence_count, ' ')
        )) as hashed_id
        FROM final_table
    )