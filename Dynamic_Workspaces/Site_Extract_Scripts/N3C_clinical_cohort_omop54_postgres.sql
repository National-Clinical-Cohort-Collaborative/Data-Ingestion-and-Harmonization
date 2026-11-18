/**
N3C Clinical Cohort (agnostic) - OMOP 5.4 - Postgres
Ported from tenant-pilot/PhenotypeScripts/n3c_clinical_omop_mssql.sql (N3C Clinical Phenotype 2.0)

HOW TO RUN:
If you are not using the R or Python exporters, you will need to find and replace @cdmDatabaseSchema and @resultsDatabaseSchema with your local OMOP 5.4 schema details. This is the only modification you should make to this script.

USER NOTES:
This script builds one table, N3C_CLINICAL_COHORT, in the results schema.
Run this script BEFORE N3C_extract_omop54_postgres.sql.
Cohort logic: a person is included if, on or after 2018-01-01, they have at least 30 days between their first and last MEASUREMENT, or between their first and last CONDITION_OCCURRENCE.
**/

CREATE TABLE IF NOT EXISTS @resultsDatabaseSchema.N3C_CLINICAL_COHORT (person_id INT NOT NULL);

TRUNCATE TABLE @resultsDatabaseSchema.N3C_CLINICAL_COHORT;

INSERT INTO @resultsDatabaseSchema.N3C_CLINICAL_COHORT
SELECT person_id
FROM @cdmDatabaseSchema.measurement
WHERE measurement_date >= DATE '2018-01-01'
GROUP BY person_id
HAVING (MAX(measurement_date) - MIN(measurement_date)) >= 30

UNION

SELECT person_id
FROM @cdmDatabaseSchema.condition_occurrence
WHERE condition_start_date >= DATE '2018-01-01'
GROUP BY person_id
HAVING (MAX(condition_start_date) - MIN(condition_start_date)) >= 30;
