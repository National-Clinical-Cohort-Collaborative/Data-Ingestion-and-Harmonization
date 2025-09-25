CREATE TABLE `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/04 - domain_mapping/care_site_2_include` AS
    with ip_npi as (
       --can not tell which is entity and which is a person 
       --SRVC_PRVDR_NPI -- The National Provider Identifier (NPI) of the health care professional who delivers or completes a particular medical service or non-surgical procedure.
    SELECT DISTINCT 
        SRVC_PRVDR_NPI as care_site_npi
        , SRVC_PRVDR_SPCLTY_CD as specialty_cd
        ,'SRVC_PRVDR_NPI' as npi_column
        ,'IP' as source_domain
      FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea`
      WHERE CHAR_LENGTH(trim(SRVC_PRVDR_NPI)) > 0
    UNION
     -- The National Provider ID (NPI) of the provider who performed the surgical procedure(s).	Operating Provider NPI
    SELECT DISTINCT 
        OPRTG_PRVDR_NPI as care_site_npi
        , BLG_PRVDR_SPCLTY_CD as specialty_cd ------------can we use the BLG_PRVDR_SPCLTY_CD code ?-----------------
        ,'OPRTG_PRVDR_NPI' as npi_column
        ,'IP' as source_domain
      FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea`
      WHERE CHAR_LENGTH(trim(OPRTG_PRVDR_NPI)) > 0
    UNION
    --ADMTG_PRVDR_NPI - empty 23.9k -- The National Provider ID (NPI) of the doctor responsible for admitting a patient to a hospital or other inpatient health facility.
    SELECT DISTINCT 
        ADMTG_PRVDR_NPI as care_site_npi
        , ADMTG_PRVDR_SPCLTY_CD as specialty_code
        ,'ADMTG_PRVDR_NPI' as npi_column
        ,'IP' as source_domain
      FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea`
      WHERE CHAR_LENGTH(trim(ADMTG_PRVDR_NPI)) > 0
    UNION
    ---- RFRG_PRVDR_NPI - empty 34.4 -- Referring provider
    SELECT DISTINCT 
        RFRG_PRVDR_NPI as care_site_npi
       -- , RFRG_PRVDR_SPCLTY_CD as specialty_code
       , cast(null as string ) as specialty_code
        ,'RFRG_PRVDR_NPI' as npi_column
        ,'IP' as source_domain
      FROM `ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea`
      WHERE CHAR_LENGTH(trim(RFRG_PRVDR_NPI)) > 0
   ),

   ip_npi_entity as (
    SELECT distinct
    ip.care_site_npi
    ,specialty_cd
    ,npi_column
    ,source_domain
    ,nppes.PROV_ORG_NAME_LEGAL_BUS_NAME
    FROM ip_npi ip
    LEFT JOIN `/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/step01_parsed/nppes_puf_20220925.csv` nppes on nppes.NPI = ip.care_site_npi     
    where nppes.PROV_GENDER_CODE NOT in ('M', 'F') and CHAR_LENGTH(trim(nppes.PROV_ORG_NAME_LEGAL_BUS_NAME)) > 0 
   )

  --other NPI columns contained provider npi not listed in the BLG_PRVDR_NPI column. Find the missing ones and add to the care_site dataset
   SELECT DISTINCT 
     care_site_npi 
     , specialty_cd
     from ip_npi_entity
     where care_site_npi not in ( select distinct BLG_PRVDR_NPI from `/UNITE/[PPRL] CMS Data & Repository/medicaid/pipeline/transform/02 - schema applied/ip`)
