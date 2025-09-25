CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/macro_visit_check` AS
   
   -- for a given patient align all visits start and end dates 
    with cteVisitTarget as (
    SELECT 
    source_domain
    , visit_occurrence_id
    , person_id as medicaid_person_id
    , visit_concept_id
    , c.concept_name
    , visit_start_date
    --if end date is null add one date to the start date  
    ,  COALESCE(NULLIF(v.visit_end_date, NULL), DATE_ADD(visit_start_date,1)) AS visit_end_date
    FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence` v
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c on v.visit_concept_id = c.concept_id and c.domain_id = 'Visit'
        --- we may want to test with one patient's data
    ---where limit to a person for testing
    ), 

    --- all dates are event_date, start_ord is the start date of 
    -- rank to add start_ordinal number 
    --------------------
    --- StartOrdinal 
    --------------------
    visitStartOrdinal as (
    select
    medicaid_person_id
    , visit_concept_id
    , visit_start_date as event_date
    , -1 as event_type
    , ROW_NUMBER() over ( PARTITION by medicaid_person_id, visit_concept_id order by visit_start_date ) as start_ordinal
    from cteVisitTarget
    union 
    -- pad the end date with 1 day to allow 1 day of difference for overlapping ranges
     select
    medicaid_person_id
    , visit_concept_id
    , DATE_ADD(visit_end_date , 1) as event_date
    , 1 as event_type
    , null
    from cteVisitTarget
    ),
    --------------------
    --- overall ordinal
    --------------------
    visitOverallOrdinal as 
    (
        SELECT
		medicaid_person_id
		, visit_concept_id
        , event_date
        , event_type
        -- this pulls the current start down from the prior rows to so that the NULLs from the END dates will contain a value we can compare with 
        , MAX( start_ordinal) OVER ( PARTITION BY medicaid_person_id, visit_concept_id order by event_date, event_type ) as start_ordinal
        -- re-number the inner union so all rows are numbered orderedd by the event date
        , ROW_NUMBER() over ( PARTITION BY medicaid_person_id, visit_concept_id order by event_date, event_type ) as overall_ord
	FROM visitStartOrdinal
    ),
    --------------------
    ---EndDates
    --------------------
    cteEndDates as (
        select 
		medicaid_person_id
		, visit_concept_id
        , date_add( event_date,  -1) as end_date -- unpad the end dates
    from visitOverallOrdinal  
    where ( 2 * start_ordinal ) - overall_ord = 0 
    )
    ---------------------------------------------------------------------------------------------------

 ---   visitEndDates as  (
    SELECT 
    v.medicaid_person_id
    , v.visit_concept_id
    , v.visit_start_date
    , MIN(e.end_date) as visit_era_end_date
    FROM  cteVisitTarget v
    JOIN cteEndDates e on v.medicaid_person_id = e.medicaid_person_id and v.visit_concept_id = e.visit_concept_id and e.end_date >= v.visit_start_date
    group by 1,2,3
