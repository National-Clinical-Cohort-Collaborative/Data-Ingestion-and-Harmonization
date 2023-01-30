CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/observation_period` AS

with obs_period AS (
    SELECT 
		  site_patient_id
		, Min(min_date) AS min_date
		, Min(min_datetime) AS min_datetime
		, Max(max_date) AS max_date
		, Max(max_datetime) AS max_datetime
		, 44814724 AS period_type_concept_id
		, data_partner_id
		, payload
	FROM (
		SELECT 
			  site_patient_id
			, data_partner_id
			, payload
            , Min(condition_start_date) AS min_date
            , Min(condition_start_datetime) AS min_datetime
            , Max(COALESCE(condition_end_date, condition_start_date)) AS max_date
            , Max(COALESCE(condition_end_datetime, condition_end_date, condition_start_date)) AS max_datetime
		FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/condition_occurrence`
		GROUP BY site_patient_id, data_partner_id, payload
		UNION 
            SELECT 
				  site_patient_id
				, data_partner_id
				, payload
                , Min(drug_exposure_start_date) AS min_date
                , Min(drug_exposure_start_datetime) AS min_datetime
                , Max(COALESCE(drug_exposure_end_date, drug_exposure_start_date)) AS max_date
                , Max(COALESCE(drug_exposure_end_datetime, drug_exposure_end_date, drug_exposure_start_date)) AS max_datetime
            FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/drug_exposure`
		    GROUP BY site_patient_id, data_partner_id, payload
		UNION 
            SELECT 
				  site_patient_id
				, data_partner_id
				, payload
                , Min(procedure_date) AS min_date
                , Min(procedure_datetime) AS min_datetime
                , Max(procedure_date) AS max_date
                , Max(procedure_datetime) AS max_datetime
            FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/procedure_occurrence`
	        GROUP BY site_patient_id, data_partner_id, payload
	    UNION
            SELECT 
				  site_patient_id
				, data_partner_id
				, payload
                , Min(observation_date) AS min_date
                , Min(observation_datetime) AS min_datetime
                , Max(observation_date) AS max_date
                , Max(observation_datetime) AS max_datetime
            FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/observation`
		    GROUP BY site_patient_id, data_partner_id, payload
		UNION 
            SELECT 
				  site_patient_id
				, data_partner_id
				, payload
                , Min(measurement_date) AS min_date
                , Min(measurement_datetime) AS min_datetime
                , Max(measurement_date) AS max_date
                , Max(measurement_datetime) AS max_datetime
            FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/measurement`
		    GROUP BY site_patient_id, data_partner_id, payload
		UNION 
		    SELECT 
				  site_patient_id
				, data_partner_id
				, payload
			    , Min(device_exposure_start_date) AS min_date
			    , Min(device_exposure_start_datetime) AS min_datetime
				, Max(COALESCE(device_exposure_end_date, device_exposure_start_date)) AS max_date
				, Max(COALESCE(device_exposure_end_datetime, device_exposure_end_date, device_exposure_start_date)) AS max_datetime
			FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/device_exposure`
		    GROUP BY site_patient_id, data_partner_id, payload
		UNION 
			SELECT 
				  site_patient_id
				, data_partner_id
				, payload
				, Min(visit_start_date) AS min_date
				, Min(visit_start_datetime) AS min_datetime
				, Max(COALESCE(visit_end_date, visit_start_date)) AS max_date
				, Max(COALESCE(visit_end_datetime, visit_end_date, visit_start_date)) AS max_datetime
			FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/visit_occurrence`
			GROUP BY site_patient_id, data_partner_id, payload
		UNION 
			SELECT
				  site_patient_id
				, data_partner_id
				, payload
				, Min(death_date) AS min_date
				, Min(death_datetime) AS min_datetime
				, Max(death_date) AS max_date
				, Max(death_datetime) AS max_datetime
			FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 777/transform/04 - domain mapping/death`
			GROUP BY site_patient_id, data_partner_id, payload
	) t
	GROUP BY t.site_patient_id, t.data_partner_id, t.payload
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247  as observation_period_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
	, hashed_id
	, site_patient_id
	, min_date as observation_period_start_date
	, min_datetime as observation_period_start_datetime
	, max_date as observation_period_end_date
	, max_datetime as observation_period_end_datetime
	, period_type_concept_id
	, data_partner_id
	, payload
FROM (
    SELECT
    	  *
    	, md5(concat_ws(
              ';'
			, COALESCE(site_patient_id, ' ')
			, COALESCE(min_date, ' ')
			, COALESCE(min_datetime, ' ')
			, COALESCE(max_date, ' ')
			, COALESCE(max_datetime, ' ')
			, COALESCE(period_type_concept_id, ' ')
        )) as hashed_id
    FROM obs_period
)