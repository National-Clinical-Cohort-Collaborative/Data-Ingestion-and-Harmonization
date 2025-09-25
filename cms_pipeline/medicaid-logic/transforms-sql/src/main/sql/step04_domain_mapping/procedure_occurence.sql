CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/procedure_occurrence` AS
   --ip/lt/ot
   --ip ICD10PCS - PRCDR_CD_1 - 6/ no hcpcs or pcs codes from LT/ ot - LINE_PRCDR_CD HCPCS code 
    WITH ip_pcs AS (
    SELECT DISTINCT
        --pkey as medicaid_person_id 
        PSEUDO_ID as medicaid_person_id
      , BLG_PRVDR_NPI as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(start_date_value, 'ddMMMyyyy') as procedure_start_date
      , to_date(end_date_value, 'ddMMMyyyy') as procedure_end_date
    , ip.src_code as procedure_source_value
    , xw.target_concept_id as procedure_concept_id
    , xw.source_concept_id as procedure_source_concept_id
    , visit_concept_id
    , 'IP' as source_domain
    , src_column
    , CAST (index as int) as src_column_index 
    FROM `ri.foundry.main.dataset.4fa0af1c-b45e-4e5e-bd29-6365da31a5a8` ip
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/03 - xwalk/codemap_medicaid_xwalk` xw
    ON ip.src_code = xw.cms_src_code AND xw.target_domain_id = 'Procedure'  and (xw.mapped_code_system = 'ICD10PCS' or xw.mapped_code_system = 'CPT4')
    WHERE ip.src_code is not NULL 
    ), 
    -- OT -- CPT or HCPCS - LINE_PRCDR_CD
    ot_hcpcs as (
    SELECT DISTINCT
        --pkey as medicaid_person_id 
        PSEUDO_ID as medicaid_person_id
      , BLG_PRVDR_NPI as care_site_npi --billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_npi 
      , to_date(SRVC_BGN_DT, 'ddMMMyyyy') as procedure_start_date
      , to_date(SRVC_END_DT, 'ddMMMyyyy') as procedure_end_date
      , ot.LINE_PRCDR_CD as procedure_source_value
    , xw.target_concept_id as procedure_concept_id
    , xw.source_concept_id as procedure_source_concept_id
      ----add the following code once the REV_CNTR_CD 
     , visit_concept_id
    , 'OT' as source_domain
    , src_column
    , CAST (1 as int) as src_column_index  -- FROM ot, the proc code will always come from the  LINE_PRCDR_CD column, so we can use 1 as the src_column_index
    FROM `ri.foundry.main.dataset.9987237b-6b2b-410d-9547-fba7aa0827e1` ot
    LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
    ON ot.src_code= xw.cms_src_code  and xw.target_domain_id = 'Procedure' and (xw.mapped_code_system = 'HCPCS' or xw.mapped_code_system = 'CPT4')
    where  ot.src_code is not null
    ),
    lt_dx as (
      SELECT DISTINCT 
      PSEUDO_ID as medicaid_person_id
      ,  COALESCE(NULLIF(BLG_PRVDR_NPI,NULL), SRVC_PRVDR_NPI) as care_site_id ----billing entity/ care_site - most likely the medical center / join with nppes_puf for Lok when building the care_site dataset
      , SRVC_PRVDR_NPI as provider_id 
      , to_date(SRVC_BGN_DT, 'ddMMMyyyy') as procedure_start_date
      , to_date(SRVC_END_DT, 'ddMMMyyyy') as procedure_end_date
      , lt.src_code as procedure_source_value
    , xw.target_concept_id as procedure_concept_id
    , xw.source_concept_id as procedure_source_concept_id
     , visit_concept_id
    , 'LT' as source_domain
    , src_column
    , CAST (index as int) as src_column_index
      FROM `ri.foundry.main.dataset.66b02062-9730-49e0-b5e1-9cc04ed76a2c` lt
      LEFT JOIN `ri.foundry.main.dataset.ec806421-244c-4e4d-814f-03a1ef171106` xw
      ON lt.src_code= xw.cms_src_code  and xw.target_domain_id = 'Procedure' and xw.mapped_code_system = 'ICD10CM'
      where lt.src_code is not null
    ),
    ip_pcs_visit as (
      SELECT DISTINCT
      ip.medicaid_person_id
    , ip.provider_npi as provider_id
    , ip.procedure_start_date
    , ip.procedure_end_date
    , ip.procedure_concept_id
    , ip.procedure_source_concept_id
    , ip.procedure_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , ip.care_site_npi as care_site_id
    , ip.source_domain
    , src_column
    , src_column_index
    , cast( null as long ) as visit_detail_id
  FROM ip_pcs ip
   JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
    ON  ip.medicaid_person_id = v.person_id 
    AND ip.procedure_start_date = v.visit_start_date
    AND ip.procedure_end_date = v.visit_end_date
    ---AND ip.provider_npi = v.provider_id
   and ip.care_site_npi = v.care_site_id ---location_id ----------
    ----and ip.visit_concept_id = v.visit_concept_id
    and v.source_domain = 'IP'
   ),
   ot_hcpcs_visit as (
    SELECT DISTINCT
      ot.medicaid_person_id
    , ot.provider_npi as provider_id
    , ot.procedure_start_date
    , ot.procedure_end_date
    , ot.procedure_concept_id
    , ot.procedure_source_concept_id
    , ot.procedure_source_value
    , v.visit_concept_id 
    , v.visit_occurrence_id
    , ot.care_site_npi as care_site_id
    , ot.source_domain
    , src_column
    , src_column_index
    , cast( null as long ) as visit_detail_id
  FROM ot_hcpcs ot
   JOIN `ri.foundry.main.dataset.b1ae35c0-342d-41ae-a1c2-5a6ddb3641fb` v
    ON  ot.medicaid_person_id = v.person_id 
    AND ot.procedure_start_date = v.visit_start_date
    AND ot.procedure_end_date = v.visit_end_date
    and ot.care_site_npi = v.care_site_id 
    and v.source_domain = 'OT'
   ), 
  lt_dx_visit as (
      SELECT DISTINCT
      lt_dx.medicaid_person_id
    , lt_dx.provider_id
    , vo.visit_start_date
    , vo.visit_end_date
    , lt_dx.procedure_concept_id
    , lt_dx.procedure_source_concept_id
    , lt_dx.procedure_source_value
    , vd.visit_detail_concept_id
    , vd.visit_occurrence_id 
    , lt_dx.care_site_id 
    , lt_dx.source_domain
    , src_column
    , src_column_index
    , cast( null as long ) as visit_detail_id --     , CAST(null as long) AS visit_detail_id -- , vd.visit_detail_id
    from lt_dx
    JOIN `ri.foundry.main.dataset.1da3baeb-61cc-428d-9728-7e001f8dde6f` vd
      on lt_dx.medicaid_person_id = vd.person_id
      and lt_dx.procedure_start_date = vd.visit_detail_start_date
      and lt_dx.procedure_end_date   = vd.visit_detail_end_date
      -- and ltdx.provider_npi = vd.provider_id 
      and lt_dx.care_site_id = vd.care_site_id ---location_id -------------
      and vd.source_domain = 'LT' 
      ---and vd.visit_concept_id = ltdx.visit_concept_id
    JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/visit_occurrence` vo
      ON vo.visit_occurrence_id = vd.visit_occurrence_id
  ),

   final_procedure as (
       select distinct * from ip_pcs_visit
       union 
       select distinct * from ot_hcpcs_visit
       union 
       SELECt DISTINCT * FROM lt_dx_visit
   ),
   medicaid_procedure as(
   select  distinct 
      md5(concat_ws(
              ';'
        , COALESCE(medicaid_person_id, '')
        , COALESCE(care_site_id, '')
        , COALESCE(provider_id, '')
        , COALESCE(procedure_start_date, '')
        , COALESCE(procedure_concept_id, '')
        , COALESCE(procedure_source_concept_id, '')
        , COALESCE(visit_concept_id, '')
        , COALESCE(source_domain, '')
        , COALESCE(visit_occurrence_id, '')
        , COALESCE( src_column, '')
        , COALESCE('medicaid', '')
        )) as medicaid_hashed_procedure_occurrence_id
        , medicaid_person_id as person_id
        , cast( procedure_concept_id as int) as procedure_concept_id
        , cast( procedure_start_date as date) as procedure_date
        , cast( procedure_start_date as timestamp ) as procedure_datetime
        , 32810 as procedure_type_concept_id
        ,cast( 0 as int) as modifier_concept_id
        ,cast( 0 as int) quantity
        , cast(provider_id as long) as provider_id
        , cast ( visit_occurrence_id as long) visit_occurrence_id
        , cast( null as long ) as visit_detail_id
        , cast( procedure_source_value as string) as procedure_source_value
        , cast( procedure_source_concept_id as int) as procedure_source_concept_id
        , cast(null as string) as modifier_source_value
       -- , case when source_column in ('LINE_PRCDR_CD', 'PRCDR_CD_1' ) then - which concept id can I use to indicate primary procedure or secondary procedure 
       --- else 0 as procedure_status_concept_id ---primary vs secondary procedure, if the code came from line or _1 then primary procedure concept id should be used
       -- post an issue to OHDSI forum - 44786630 = primary procedure type is not same as the primary procedure status concept id
       -- Until OHDSI CDM confirms a procedure status concept id we can use we will map to 0 for now, shong 4/28/2023
        , src_column_index as procedure_status_concept_id       
        , src_column as procedure_status_source_value ---condition_status_source_value
        , source_domain
  FROM final_procedure
  where procedure_concept_id is not null
  )
 
 SELECT
    p.*
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    , cast(conv(substr(medicaid_hashed_procedure_occurrence_id, 1, 15), 16, 10) as bigint) as procedure_occurrence_id
    , m.macro_visit_long_id
    FROM medicaid_procedure p
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/macro_visit_with_micro_visit` m on m.visit_occurrence_id = p.visit_occurrence_id
