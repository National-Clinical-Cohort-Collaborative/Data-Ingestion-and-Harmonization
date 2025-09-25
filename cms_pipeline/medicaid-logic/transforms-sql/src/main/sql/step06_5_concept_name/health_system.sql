CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/06_5 - add concept name/health_system` AS
    SELECT h.health_system_id
    ,h.teaching_intensity
    ,h.teaching_intensity_category
    ,h.multistate
    ,h.multistate_category
    ,h.care_site_size
    ,h.care_site_size_category
    ,h.care_site_name
    ,h.place_of_service_concept_id
    ,h.location_id
    ,h.care_site_source_value
    ,h.place_of_service_source_value
    ,h.care_site_year
    ,c.care_site_id as care_site_id
    ,h.health_system_hashed_id

     FROM `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/06 - schema check/health_system` h
    left join `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/05 - safe/care_site` c  on h.care_site_id = c.orig_care_site_id