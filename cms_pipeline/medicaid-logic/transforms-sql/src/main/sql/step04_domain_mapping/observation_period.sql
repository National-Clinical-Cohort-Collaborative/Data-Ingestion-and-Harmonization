CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/observation_period` AS
    
    --defines a span of time during which clinical events are recorded
    with obs_period AS (
    SELECT 
		  person_id
		, Min(min_date) AS min_date
		, Min(min_datetime) AS min_datetime
		, Max(max_date) AS max_date
		, Max(max_datetime) AS max_datetime
		, 32810 AS period_type_concept_id
		, 'medicaid' as data_partner_id
	FROM (
		SELECT 
			  person_id
            , Min(condition_start_date) AS min_date
            , Min(condition_start_datetime) AS min_datetime
            , Max(COALESCE(condition_end_date, condition_start_date)) AS max_date
            , Max(COALESCE(condition_end_datetime, condition_end_date, condition_start_date)) AS max_datetime
		FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/condition_occurrence`
		GROUP BY person_id
		UNION 
            SELECT 
				  person_id
                , Min(drug_exposure_start_date) AS min_date
                , Min(drug_exposure_start_datetime) AS min_datetime
                , Max(COALESCE(drug_exposure_end_date, drug_exposure_start_date)) AS max_date
                , Max(COALESCE(drug_exposure_end_datetime, drug_exposure_end_date, drug_exposure_start_date)) AS max_datetime
            FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/drug_exposure`
		    GROUP BY person_id
		UNION 
            SELECT 
				  person_id
                , Min(procedure_date) AS min_date
                , Min(procedure_datetime) AS min_datetime
                , Max(procedure_date) AS max_date
                , Max(procedure_datetime) AS max_datetime
            FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/procedure_occurrence`
	        GROUP BY person_id
	    UNION
            SELECT 
				  person_id
                , Min(observation_date) AS min_date
                , Min(observation_datetime) AS min_datetime
                , Max(observation_date) AS max_date
                , Max(observation_datetime) AS max_datetime
            FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/observation`
		    GROUP BY person_id
            --TODO: add measurement when ready
		-- UNION 
        --     SELECT 
		-- 		  person_id
        --         , Min(measurement_date) AS min_date
        --         , Min(measurement_datetime) AS min_datetime
        --         , Max(measurement_date) AS max_date
        --         , Max(measurement_datetime) AS max_datetime
        --     FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/measurement`
		--     GROUP BY person_id, data_partner_id, payload
		UNION 
		    SELECT 
				  person_id
			    , Min(device_exposure_start_date) AS min_date
			    , Min(device_exposure_start_datetime) AS min_datetime
				, Max(COALESCE(device_exposure_end_date, device_exposure_start_date)) AS max_date
				, Max(COALESCE(device_exposure_end_datetime, device_exposure_end_date, device_exposure_start_date)) AS max_datetime
			FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/device_exposure`
		    GROUP BY person_id
		UNION 
			SELECT 
				  person_id
				, Min(visit_start_date) AS min_date
				, Min(visit_start_datetime) AS min_datetime
				, Max(COALESCE(visit_end_date, visit_start_date)) AS max_date
				, Max(COALESCE(visit_end_datetime, visit_end_date, visit_start_date)) AS max_datetime
			FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence`
			GROUP BY person_id
		UNION 
			SELECT
				  person_id
				, Min(death_date) AS min_date
				, Min(death_datetime) AS min_datetime
				, Max(death_date) AS max_date
				, Max(death_datetime) AS max_datetime
			FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/death`
			GROUP BY person_id
	) t
	GROUP BY t.person_id
)

SELECT
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) as observation_period_id
    -- Pass through the hashed id to join on lookup table in case of conflicts
	, hashed_id
	, person_id
	, min_date as observation_period_start_date
	, min_datetime as observation_period_start_datetime
	, max_date as observation_period_end_date
	, max_datetime as observation_period_end_datetime
	, period_type_concept_id
	, data_partner_id
FROM (
    SELECT
    	  *
    	, md5(concat_ws(
              ';'
			, COALESCE(person_id, ' ')
			, COALESCE(min_date, ' ')
			, COALESCE(min_datetime, ' ')
			, COALESCE(max_date, ' ')
			, COALESCE(max_datetime, ' ')
			, COALESCE(period_type_concept_id, ' ')
        )) as hashed_id
    FROM obs_period
)