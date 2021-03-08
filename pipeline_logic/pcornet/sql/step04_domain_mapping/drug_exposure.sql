CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/04 - domain mapping/drug_exposure` AS

with condition as (
    SELECT
        patid AS site_patid,
        CAST(xw.target_concept_id as int) as drug_concept_id,
        CAST(c.report_date as date) as drug_exposure_start_date,
        CAST(null as timestamp) as drug_exposure_start_datetime,
        CAST(COALESCE(c.resolve_date, c.report_date) as date) as drug_exposure_end_date, --sn 6/28/2020
        CAST(null as timestamp) as drug_exposure_end_datetime,
        CAST(null as date) as verbatim_end_date,
        -- 45769798 as drug_type_concept_id, --ssh 6/25/2020 --issue number 54
        32817 as drug_type_concept_id, 
        CAST(null as string) AS stop_reason,
        CAST(null as int) AS refills,
        CAST(null as float) AS quantity,
        CAST(null AS int) AS days_supply,
        CAST(null AS string) AS sig,
        CAST(null AS int) AS route_concept_id,
        CAST(null AS string) AS lot_number,
        CAST(null AS int) AS provider_id, --m.medadmin_providerid as provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        CAST(c.condition as string) as drug_source_value,
        CAST(xw.source_concept_id as int) as drug_source_concept_id,
        CAST(null AS string) as route_source_value,
        CAST(null AS string) as dose_unit_source_value,
        'CONDITION' as domain_source,
        conditionid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/condition` c
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
            ON c.condition = xw.src_code 
            AND xw.CDM_TBL = 'CONDITION' AND xw.target_domain_id = 'Drug'
            AND xw.src_code_type = c.condition_type
),

diagnosis as (
    SELECT 
        patid AS site_patid,  
        CAST(xw.target_concept_id as int) as drug_concept_id,
        CAST(COALESCE(d.dx_date, d.admit_date) as date) as drug_exposure_start_date, 
        CAST(null as timestamp) as drug_exposure_start_datetime,
        CAST(COALESCE(d.dx_date, d.admit_date) as date) as drug_exposure_end_date, --need to be revisited
        CAST(null as timestamp) as drug_exposure_end_datetime, --need to be revisited
        CAST(null as date) as verbatim_end_date,
        -- 581373 as drug_type_concept_id, -- medication administered to patient, from DX_ORIGIN: code 'OD','BI','CL','DR','NI','UN,'OT'
        -- case when d.DX_ORIGIN = 'OD' then 38000179
        --     when d.DX_ORIGIN = 'BI' then 38000177
        --     when d.DX_ORIGIN = 'CL' then 38000177
        -- else 45769798 end as drug_type_concept_id, --added on 6/26
        CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) AS drug_type_concept_id,
        CAST(null as string) AS stop_reason,
        CAST(null as int) AS refills,
        CAST(null as float) AS quantity,
        CAST(null AS int) AS days_supply,
        CAST(null AS string) AS sig,
        CAST(null AS int) AS route_concept_id,
        CAST(null AS string) AS lot_number,
        CAST(null AS int) AS provider_id, --m.medadmin_providerid as provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        CAST(d.dx as string) as drug_source_value,
        -- null as drug_source_concept_id, --**MB: replace this with below line to populate with xw.source_concept_id
        CAST(xw.source_concept_id as int) as drug_source_concept_id,
        CAST(null AS string) as route_source_value, 
        CAST(null AS string) as dose_unit_source_value, 
        'DIAGNOSIS' as domain_source,
        diagnosisid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/diagnosis` d 
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
            ON d.dx = xw.src_code 
            AND xw.CDM_TBL = 'DIAGNOSIS' AND xw.target_domain_id = 'Drug' 
            AND xw.src_code_type = d.dx_type
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
            ON d.dx_origin = xw2.SRC_CODE
            AND xw2.CDM_TBL = 'DIAGNOSIS' AND xw2.CDM_TBL_COLUMN_NAME = 'dx_origin'
), 

med_admin as (
    SELECT 
        patid AS site_patid,
        CAST(xw.target_concept_id as int) as drug_concept_id,
        CAST(m.medadmin_start_date as date) as drug_exposure_start_date, 
        CAST(m.MEDADMIN_START_DATETIME as timestamp) as drug_exposure_start_datetime,
        -- NVL2(m.medadmin_start_date,TO_DATE(TO_CHAR(m.medadmin_start_date, 'DD-MON-YYYY') ||' '|| m.medadmin_start_time, 'DD-MON-YYYY HH24MISS'),null) as drug_exposure_start_datetime, 
        CAST(m.medadmin_stop_date as date) as drug_exposure_end_date,
        CAST(m.MEDADMIN_STOP_DATETIME as timestamp) as drug_exposure_end_datetime, 
        -- NVL2((isNull(m.medadmin_stop_date) or isNull(m.medadmin_stop_date)),TO_DATE(TO_CHAR(m.medadmin_stop_date, 'DD-MON-YYYY') ||' '|| m.medadmin_stop_time, 'DD-MON-YYYY HH24MISS'),null) as drug_exposure_end_datetime,
        CAST(null as date) as verbatim_end_date,
        -- 581373 as drug_type_concept_id, --m medication administered to patient 
        32817 as drug_type_concept_id,
        CAST(null as string) AS stop_reason,
        CAST(null as int) AS refills,
        CAST(null as float) AS quantity,
        CAST(null AS int) AS days_supply,
        CAST(xw.source_code_description as string) as sig,
        CAST(r.TARGET_CONCEPT_ID as int) as route_concept_id,
        CAST(null AS string) AS lot_number,
        CAST(null AS int) AS provider_id, --m.medadmin_providerid as provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        CAST(m.medadmin_code as string) as drug_source_value,
        CAST(xw.source_concept_id as int) as drug_source_concept_id,
        CAST(m.medadmin_route as string) as route_source_value,
        CAST(m.medadmin_dose_admin_unit as string) as dose_unit_source_value,
        'MED_ADMIN' as domain_source,
        medadminid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/med_admin` m
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
            ON m.medadmin_code = xw.src_code 
            AND xw.CDM_TBL = 'MED_ADMIN' AND xw.target_domain_id = 'Drug'  
            AND xw.src_code_type = m.medadmin_type --added on 6/28
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` r 
            ON m.medadmin_route = r.SRC_CODE and r.CDM_TBL_COLUMN_NAME = 'RX_ROUTE'
),

prescribing as (
    SELECT
        patid AS site_patid,
        CAST(xw.target_concept_id as int) drug_concept_id,
        CAST(COALESCE(rx_start_date, rx_order_date) as date) AS drug_exposure_start_date,
        CAST(null as timestamp) AS drug_exposure_start_datetime, 
        -- COALESCE(rx_end_date, rx_order_date) as drug_exposure_end_date, 
        -- COALESCE(rx_end_date,rx_order_date) as drug_exposure_end_datetime,
        -- CY: obo CB: if the date is null set the drug_exposure_end_date to rx_start_date + rx_days_supply -1. 
        -- If rx_days_supply is null, set the drug_exposure_end_date = rx_start_date.
        CASE
            WHEN rx_end_date IS NULL 
            THEN
                CASE
                    WHEN COALESCE(rx_days_supply, 0) = 0 
                        THEN CAST(COALESCE(rx_start_date, rx_order_date) as date)
                    ELSE DATE_ADD(COALESCE(rx_start_date, rx_order_date), (rx_days_supply - 1))
                END
            ELSE rx_end_date
        END AS drug_exposure_end_date,
        -- CAST(null as date) as drug_exposure_end_date,
        CAST(null as timestamp) as drug_exposure_end_datetime,
        CAST(pr.rx_end_date as date) AS verbatim_end_date,
        -- 38000177 AS drug_type_concept_id,
        32817 AS drug_type_concept_id,
        CAST(null as string) AS stop_reason,
        CAST(null as int) AS refills,
        CAST(null as float) AS quantity,
        CAST(rx_days_supply as int) AS days_supply,
        CAST(rx_frequency as string) AS sig,
        CAST(mx.TARGET_CONCEPT_ID as int) AS route_concept_id,
        CAST(null AS string) AS lot_number,
        -- prv.n3cds_domain_map_id AS provider_id,
        CAST(null AS int) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        CAST(rxnorm_cui as string) AS drug_source_value,
        CAST(xw.source_concept_id as int) AS drug_source_concept_id, --- drug source concept id if it is prescribing
        CAST(rx_route as string) AS route_source_value,
        CAST(rx_dose_ordered_unit as string) AS dose_unit_source_value,
        'PRESCRIBING' AS domain_source,
        prescribingid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/prescribing` pr
            JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
                ON rxnorm_cui = xw.src_code
                AND xw.src_code_type = 'rxnorm_cui'
                AND xw.CDM_TBL = 'PRESCRIBING'
                AND xw.target_domain_id = 'Drug'
            LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` mx 
                ON mx.CDM_TBL_COLUMN_NAME = 'RX_ROUTE'
                AND mx.SRC_CODE = pr.rx_route
            LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` u 
                ON pr.rx_dose_ordered_unit = u.SRC_CODE
                AND u.CDM_TBL_COLUMN_NAME = 'RX_DOSE_ORDERED_UNIT'
),

procedures as (
    SELECT
        patid AS site_patid,
        CAST(xw.target_concept_id as int) AS drug_concept_id,
        CAST(pr.px_date as date) AS drug_exposure_start_date,
        CAST(null as timestamp) AS drug_exposure_start_datetime,
        CAST(pr.px_date as date) AS drug_exposure_end_date,
        CAST(null as timestamp) AS drug_exposure_end_datetime,
        CAST(null as date) as verbatim_end_date,
        -- xw2.TARGET_CONCEPT_ID as drug_type_concept_id,
        -- 38000179 AS drug_type_concept_id,
        CAST(COALESCE(xw2.TARGET_CONCEPT_ID, 0) as int) AS drug_type_concept_id,
        CAST(null as string) AS stop_reason,
        CAST(null as int) AS refills,
        CAST(null as float) AS quantity,
        CAST(null AS int) AS days_supply,
        CAST(null AS string) AS sig,
        CAST(null AS int) AS route_concept_id,
        CAST(null AS string) AS lot_number,
        CAST(null AS int) AS provider_id,
        encounterid as site_encounterid,
        CAST(null as int) AS visit_detail_id,
        CAST(pr.px as string) AS drug_source_value,
        CAST(xw.source_concept_id as int) AS drug_source_concept_id,
        CAST(null AS string) AS route_source_value,
        CAST(null AS string) AS dose_unit_source_value,
        'PROCEDURES' AS domain_source,
        proceduresid as site_pkey,
        data_partner_id,
        payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/procedures` pr
         --this is to look for drug_concept_id line 27 can use mp.target_concept_id, makes no difference
        INNER JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/p2o_code_xwalk` xw 
            ON pr.px = xw.src_code
            AND xw.CDM_TBL = 'PROCEDURES' AND xw.target_domain_id = 'Drug' 
            AND xw.src_code_type = pr.px_type
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/p2o_valueset_mapping_table` xw2 
            ON pr.px_source = xw2.SRC_CODE 
            AND xw2.CDM_TBL = 'PROCEDURES' AND xw2.CDM_TBL_COLUMN_NAME = 'PX_SOURCE'                                                       
),

all_domains as (
    SELECT * FROM (
        SELECT * FROM condition 
            UNION ALL  
        SELECT * FROM diagnosis 
            UNION ALL 
        SELECT * FROM med_admin 
            UNION ALL 
        SELECT * FROM prescribing 
            UNION ALL 
        SELECT * FROM procedures
    )
    WHERE drug_concept_id IS NOT NULL
),

final_table AS (
    SELECT
        *
        -- Required for identical rows so that their IDs differ when hashing
        , row_number() OVER (
            PARTITION BY
              site_patid
            , drug_concept_id
            , drug_exposure_start_date
            , drug_exposure_start_datetime
            , drug_exposure_end_date
            , drug_exposure_end_datetime
            , verbatim_end_date
            , drug_type_concept_id
            , stop_reason
            , refills
            , quantity
            , days_supply
            , sig
            , route_concept_id
            , lot_number
            , provider_id
            , site_encounterid
            , visit_detail_id
            , drug_source_value
            , drug_source_concept_id
            , route_source_value
            , dose_unit_source_value   
            ORDER BY site_patid        
        ) as row_index
    FROM all_domains
)

SELECT 
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_exposure_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patid
    , drug_concept_id
    , drug_exposure_start_date
    , drug_exposure_start_datetime
    , drug_exposure_end_date
    , drug_exposure_end_datetime
    , verbatim_end_date
    , drug_type_concept_id
    , stop_reason
    , refills
    , quantity
    , days_supply
    , sig
    , route_concept_id
    , lot_number
    , provider_id
    , site_encounterid
    , visit_detail_id
    , drug_source_value
    , drug_source_concept_id
    , route_source_value
    , dose_unit_source_value
    , domain_source
    , site_pkey
    , data_partner_id
    , payload
FROM (
    SELECT
        *
    , md5(concat_ws(
            ';'
            , COALESCE(site_patid, '')
            , COALESCE(drug_concept_id, '')
            , COALESCE(drug_exposure_start_date, '')
            , COALESCE(drug_exposure_start_datetime, '')
            , COALESCE(drug_exposure_end_date, '')
            , COALESCE(drug_exposure_end_datetime, '')
            , COALESCE(verbatim_end_date, '')
            , COALESCE(drug_type_concept_id, '')
            , COALESCE(stop_reason, '')
            , COALESCE(refills, '')
            , COALESCE(quantity, '')
            , COALESCE(days_supply, '')
            , COALESCE(sig, '')
            , COALESCE(route_concept_id, '')
            , COALESCE(lot_number, '')
            , COALESCE(provider_id, '')
            , COALESCE(site_encounterid, '')
            , COALESCE(visit_detail_id, '')
            , COALESCE(drug_source_value, '')
            , COALESCE(drug_source_concept_id, '')
            , COALESCE(route_source_value, '')
            , COALESCE(dose_unit_source_value, '')
            , COALESCE(row_index, '')
            , site_pkey
    )) as hashed_id
    FROM final_table
)
