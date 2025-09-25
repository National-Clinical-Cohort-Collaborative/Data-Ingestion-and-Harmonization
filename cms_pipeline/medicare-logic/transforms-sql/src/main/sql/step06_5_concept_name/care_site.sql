
    CREATE TABLE `ri.foundry.main.dataset.08bb334c-3164-4c0b-a58b-eed1e46ee292` AS

    SELECT
        care_site_id,
        care_site_name,
        place_of_service_concept_id,
        location_id,
        care_site_source_value,
        place_of_service_source_value,
        data_partner_id
    FROM `ri.foundry.main.dataset.27dca808-87ca-43df-9dd4-cc698b38386a` c

    